package firecrest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func newTestServer(t *testing.T, h http.HandlerFunc) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var tokens atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("client_id") != "cid" || r.Form.Get("client_secret") != "sec" {
			http.Error(w, "bad client", http.StatusUnauthorized)
			return
		}
		tokens.Add(1)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"access_token":"tok","token_type":"Bearer","expires_in":300}`)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" && r.Header.Get("X-API-Key") != "key" {
			http.Error(w, `{"message":"no auth"}`, http.StatusUnauthorized)
			return
		}
		h(w, r)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &tokens
}

func newTestClient(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()
	c, err := New(context.Background(), Options{
		URL: srv.URL, System: "sys", TokenURL: srv.URL + "/token", ClientID: "cid", ClientSecret: "sec",
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSubmitJobSendsSpecAndReusesToken(t *testing.T) {
	var got map[string]JobSpec
	srv, tokens := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/compute/sys/jobs" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusCreated)
		io.WriteString(w, `{"jobId": 42}`)
	})
	c := newTestClient(t, srv)
	for i := 0; i < 3; i++ {
		id, err := c.SubmitJob(context.Background(), JobSpec{ScriptPath: "/r/job.slurm", WorkingDirectory: "/r"})
		if err != nil {
			t.Fatal(err)
		}
		if id != "42" {
			t.Fatalf("jobId = %q, want 42", id)
		}
	}
	if got["job"].ScriptPath != "/r/job.slurm" || got["job"].WorkingDirectory != "/r" {
		t.Fatalf("body = %+v", got)
	}
	if n := tokens.Load(); n != 1 {
		t.Fatalf("token fetched %d times, want 1", n)
	}
}

func TestGetJobAndNotFound(t *testing.T) {
	srv, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/compute/sys/jobs/7":
			io.WriteString(w, `{"jobs":[{"jobId":"7","status":{"state":"CANCELLED by 1001","exitCode":0,"interruptSignal":15},"nodes":"nid001"}]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"errorType":"error","message":"Job not found."}`)
		}
	})
	c := newTestClient(t, srv)
	j, err := c.GetJob(context.Background(), "7")
	if err != nil {
		t.Fatal(err)
	}
	if j.Status.State != "CANCELLED by 1001" || j.Status.InterruptSignal != 15 || j.Nodes != "nid001" {
		t.Fatalf("job = %+v", j)
	}
	_, err = c.GetJob(context.Background(), "8")
	if !IsNotFound(err) {
		t.Fatalf("err = %v, want not found", err)
	}
	if !strings.Contains(err.Error(), "Job not found.") {
		t.Fatalf("error text %q does not carry the FirecREST message", err)
	}
}

func TestUploadIsMultipartIntoDir(t *testing.T) {
	srv, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/filesystem/sys/ops/upload" || r.URL.Query().Get("path") != "/r/d" {
			t.Errorf("unexpected %s ?%s", r.URL.Path, r.URL.RawQuery)
		}
		f, hdr, err := r.FormFile("file")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(f)
		if hdr.Filename != "job.sh" || string(b) != "echo hi\n" {
			t.Errorf("file %q = %q", hdr.Filename, b)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	c := newTestClient(t, srv)
	if err := c.Upload(context.Background(), "/r/d", "job.sh", []byte("echo hi\n")); err != nil {
		t.Fatal(err)
	}
}

func TestViewAndLsDecode(t *testing.T) {
	srv, _ := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch r.URL.Path {
		case "/filesystem/sys/ops/view":
			if q.Get("offset") != "6" || q.Get("size") != "100" {
				t.Errorf("view query %s", r.URL.RawQuery)
			}
			io.WriteString(w, `{"output":"world\n"}`)
		case "/filesystem/sys/ops/ls":
			if q.Get("recursive") != "true" || q.Get("showHidden") != "true" {
				t.Errorf("ls query %s", r.URL.RawQuery)
			}
			io.WriteString(w, `{"output":[{"name":"sub/a.out","type":"-","size":"12","lastModified":"2026-10-02T15:47:25"}]}`)
		}
	})
	c := newTestClient(t, srv)
	b, err := c.View(context.Background(), "/r/x", 6, 100)
	if err != nil || string(b) != "world\n" {
		t.Fatalf("view = %q, %v", b, err)
	}
	es, err := c.Ls(context.Background(), "/r", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(es) != 1 || es[0].Name != "sub/a.out" || es[0].Size.Int64() != 12 {
		t.Fatalf("ls = %+v", es)
	}
}

func TestAPIKeyAuth(t *testing.T) {
	srv, tokens := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"user":{"name":"svc"}}`)
	})
	c, err := New(context.Background(), Options{URL: srv.URL, System: "sys", APIKey: "key"})
	if err != nil {
		t.Fatal(err)
	}
	u, err := c.UserInfo(context.Background())
	if err != nil || u != "svc" {
		t.Fatalf("userinfo = %q, %v", u, err)
	}
	if tokens.Load() != 0 {
		t.Fatal("API key auth must not fetch a token")
	}
}

func TestNewRequiresAuth(t *testing.T) {
	if _, err := New(context.Background(), Options{URL: "http://x", System: "s"}); err == nil {
		t.Fatal("expected an error without credentials")
	}
}
