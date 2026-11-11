package firecrest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// What FirecREST answers when the command behind a call outlives its SSH
// timeout, as seen at a site during an upload.
const timeoutBody = `{"errorType":"error","message":"Command execution timeout limit exceeded."}`

// newRetryClient is newTestClient with retries a few milliseconds apart; log
// collects what the client logs.
func newRetryClient(t *testing.T, srv *httptest.Server, timeout time.Duration) (*Client, *bytes.Buffer) {
	t.Helper()
	var log bytes.Buffer
	c, err := New(context.Background(), Options{
		URL: srv.URL, System: "sys", TokenURL: srv.URL + "/token", ClientID: "cid", ClientSecret: "my-secret",
		Timeout: timeout,
		Retry: Retry{
			Attempts: 4, Delay: time.Millisecond, MaxElapsed: 5 * time.Second,
			Log: slog.New(slog.NewTextHandler(&log, nil)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return c, &log
}

// failFirst answers the first n calls with status and body, the rest with ok.
func failFirst(n int32, status int, body string, ok http.HandlerFunc) (http.HandlerFunc, *atomic.Int32) {
	var calls atomic.Int32
	return func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= n {
			w.WriteHeader(status)
			io.WriteString(w, body)
			return
		}
		ok(w, r)
	}, &calls
}

// The upload that failed a job's submission: one 504, then FirecREST works.
func TestUploadRetries504(t *testing.T) {
	h, calls := failFirst(1, http.StatusGatewayTimeout, timeoutBody, func(w http.ResponseWriter, r *http.Request) {
		f, _, err := r.FormFile("file")
		if err != nil {
			t.Error(err)
			return
		}
		if b, _ := io.ReadAll(f); string(b) != "echo hi\n" {
			t.Errorf("retried upload sent %q", b)
		}
		w.WriteHeader(http.StatusCreated)
	})
	srv, _ := newTestServer(t, h)
	c, log := newRetryClient(t, srv, 0)
	if err := c.Upload(context.Background(), "/r/d", "job.sh", []byte("echo hi\n")); err != nil {
		t.Fatalf("upload after one 504: %v", err)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("%d calls, want 2", n)
	}
	if n := strings.Count(log.String(), "retrying"); n != 1 {
		t.Fatalf("%d retry lines, want 1:\n%s", n, log)
	}
	if !strings.Contains(log.String(), "/filesystem/sys/ops/upload") || !strings.Contains(log.String(), "Command execution timeout limit exceeded.") {
		t.Fatalf("retry line does not name the call and the error:\n%s", log)
	}
}

// Every call of the status and sync path is retried on 502, 503 and 504.
func TestReadsRetry5xx(t *testing.T) {
	for _, status := range []int{http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			h, calls := failFirst(2, status, `{"message":"busy"}`, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/compute/sys/jobs/7":
					io.WriteString(w, `{"jobs":[{"jobId":7,"status":{"state":"RUNNING"}}]}`)
				case "/filesystem/sys/ops/view":
					io.WriteString(w, `{"output":"log\n"}`)
				case "/filesystem/sys/ops/ls":
					io.WriteString(w, `{"output":[]}`)
				default:
					w.WriteHeader(http.StatusOK)
					io.WriteString(w, `{}`)
				}
			})
			srv, _ := newTestServer(t, h)
			c, _ := newRetryClient(t, srv, 0)
			ctx := context.Background()
			for _, tc := range []struct {
				name string
				run  func() error
			}{
				{"GetJob", func() error { _, err := c.GetJob(ctx, "7"); return err }},
				{"View", func() error { _, err := c.View(ctx, "/r/job.out", 0, 10); return err }},
				{"Ls", func() error { _, err := c.Ls(ctx, "/r", false, true); return err }},
				{"Mkdir -p", func() error { return c.Mkdir(ctx, "/r/a/b", true) }},
				{"Chmod", func() error { return c.Chmod(ctx, "/r/job.sh", "755") }},
				{"CancelJob", func() error { return c.CancelJob(ctx, "7") }},
				{"Rm", func() error { return c.Rm(ctx, "/r") }},
			} {
				calls.Store(0)
				if err := tc.run(); err != nil {
					t.Errorf("%s after two %d: %v", tc.name, status, err)
				}
				if n := calls.Load(); n != 3 {
					t.Errorf("%s: %d calls, want 3", tc.name, n)
				}
			}
		})
	}
}

// A 4xx is an answer, not a hiccup: never sent again.
func TestNo4xxRetry(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusNotFound, http.StatusFailedDependency, http.StatusTooManyRequests} {
		var calls atomic.Int32
		srv, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(status)
			io.WriteString(w, `{"message":"no"}`)
		})
		c, log := newRetryClient(t, srv, 0)
		if _, err := c.GetJob(context.Background(), "7"); err == nil {
			t.Errorf("%d: no error", status)
		}
		if n := calls.Load(); n != 1 || log.Len() != 0 {
			t.Errorf("%d: %d calls, log %q; want 1 call and no retry", status, n, log)
		}
	}
}

// FirecREST may have queued a job whose submission timed out, so SubmitJob
// leaves the decision to its caller; a plain mkdir that worked would fail.
func TestSubmitAndPlainMkdirAreSentOnce(t *testing.T) {
	var calls atomic.Int32
	srv, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusGatewayTimeout)
		io.WriteString(w, timeoutBody)
	})
	c, _ := newRetryClient(t, srv, 0)
	_, err := c.SubmitJob(context.Background(), JobSpec{ScriptPath: "/r/job.slurm", WorkingDirectory: "/r"})
	if !Transient(err) || NotSent(err) {
		t.Fatalf("submit error %v: want transient and possibly sent", err)
	}
	if err := c.Mkdir(context.Background(), "/r/x", false); err == nil {
		t.Fatal("plain mkdir: no error")
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("%d calls, want 1 submission and 1 mkdir", n)
	}
}

// Attempts bounds the tries, MaxElapsed the time.
func TestRetryIsBounded(t *testing.T) {
	var calls atomic.Int32
	srv, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	c, log := newRetryClient(t, srv, 0)
	if _, err := c.UserInfo(context.Background()); err == nil {
		t.Fatal("no error from a FirecREST that always answers 503")
	}
	if n := calls.Load(); n != 4 {
		t.Fatalf("%d calls, want 4", n)
	}
	if n := strings.Count(log.String(), "retrying"); n != 3 {
		t.Fatalf("%d retry lines, want 3", n)
	}

	calls.Store(0)
	c.retry = Retry{Attempts: 10, Delay: 40 * time.Millisecond, MaxElapsed: 100 * time.Millisecond}
	start := time.Now()
	if _, err := c.UserInfo(context.Background()); err == nil {
		t.Fatal("no error")
	}
	// Waits of 40 and 80 ms: the second would end after 100 ms.
	if n, took := calls.Load(), time.Since(start); n != 2 || took > time.Second {
		t.Fatalf("%d calls in %s, want 2 well within a second", n, took)
	}

	calls.Store(0)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	c.retry = Retry{Attempts: 10, Delay: time.Second, MaxElapsed: time.Minute}
	if _, err := c.UserInfo(ctx); err == nil || calls.Load() != 1 {
		t.Fatalf("a wait past the caller's deadline: %d calls, %v", calls.Load(), err)
	}
}

// A connection that drops before the answer, and one that times out, are
// tried again. An upload, since Go's transport itself resends a GET on a
// connection reset.
func TestRetriesResetAndTimeout(t *testing.T) {
	var calls atomic.Int32
	srv, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
		}
		switch calls.Add(1) {
		case 1:
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			if tcp, ok := conn.(*net.TCPConn); ok {
				tcp.SetLinger(0) // RST, not FIN
			}
			conn.Close()
			return
		case 2:
			time.Sleep(300 * time.Millisecond)
		}
		w.WriteHeader(http.StatusCreated)
	})
	c, log := newRetryClient(t, srv, 100*time.Millisecond)
	if err := c.Upload(context.Background(), "/r/d", "job.sh", []byte("echo hi\n")); err != nil {
		t.Fatalf("upload: %v\n%s", err, log)
	}
	if n := calls.Load(); n != 3 {
		t.Fatalf("%d calls, want 3:\n%s", n, log)
	}
	if n := strings.Count(log.String(), "retrying"); n != 2 {
		t.Fatalf("%d retry lines, want 2:\n%s", n, log)
	}
}

// The token endpoint is on every call's path too.
func TestTokenEndpoint503IsRetried(t *testing.T) {
	var tokenCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if tokenCalls.Add(1) == 1 {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"access_token":"t","token_type":"Bearer","expires_in":300}`)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"user":{"name":"fireuser"}}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c, _ := newRetryClient(t, srv, 0)
	if _, err := c.UserInfo(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := tokenCalls.Load(); n != 2 {
		t.Fatalf("%d token calls, want 2", n)
	}
}

func TestTransientAndNotSent(t *testing.T) {
	dial := &url.Error{Op: "Post", URL: "https://f7t", Err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}}
	reset := &url.Error{Op: "Post", URL: "https://f7t", Err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}}
	for _, tc := range []struct {
		name               string
		err                error
		transient, notSent bool
	}{
		{"504", &APIError{Status: 504}, true, false},
		{"500", &APIError{Status: 500}, false, false},
		{"404", &APIError{Status: 404}, false, false},
		{"refused", dial, true, true},
		{"reset", reset, true, false},
		{"EOF", &url.Error{Op: "Post", URL: "https://f7t", Err: io.EOF}, true, false},
		{"canceled", &url.Error{Op: "Post", URL: "https://f7t", Err: context.Canceled}, false, false},
		{"other", errors.New("decoding"), false, false},
	} {
		if got := Transient(tc.err); got != tc.transient {
			t.Errorf("Transient(%s) = %v", tc.name, got)
		}
		if got := NotSent(tc.err); got != tc.notSent {
			t.Errorf("NotSent(%s) = %v", tc.name, got)
		}
	}
}

func TestJobsAsksByName(t *testing.T) {
	srv, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/compute/sys/jobs" || r.URL.Query().Get("name") != "uid-1" {
			t.Errorf("unexpected %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		io.WriteString(w, `{"jobs":[{"jobId":12,"name":"uid-1","workingDirectory":"/r/ns-uid-1","status":{"state":"PENDING"}}]}`)
	})
	c, _ := newRetryClient(t, srv, 0)
	jobs, err := c.Jobs(context.Background(), "uid-1")
	if err != nil || len(jobs) != 1 || jobs[0].JobID != "12" || jobs[0].WorkingDirectory != "/r/ns-uid-1" {
		t.Fatalf("jobs = %+v, %v", jobs, err)
	}
}
