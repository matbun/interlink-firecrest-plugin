package bridge

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/matbun/interlink-firecrest-plugin/pkg/firecrest"
)

// What FirecREST answers when sbatch, or an upload, outlives its SSH timeout.
func timedOut(path string) error {
	return &firecrest.APIError{Method: "POST", Path: path, Status: http.StatusGatewayTimeout,
		Body: `{"message":"Command execution timeout limit exceeded."}`}
}

// namedPodDir is podDir with the job name the plugin writes: the pod's UID.
func (f *fixture) namedPodDir(name, uid string) string {
	f.t.Helper()
	dir := f.podDir(name)
	script := "#!/bin/bash\n#SBATCH --job-name=" + uid + "\n#SBATCH --output=" + dir + "/job.out\n" + dir + "/job.sh\n"
	if err := os.WriteFile(filepath.Join(dir, "job.slurm"), []byte(script), 0o774); err != nil {
		f.t.Fatal(err)
	}
	return dir
}

// A submission that timed out after FirecREST queued the job: the bridge finds
// that job and does not queue a second one.
func TestSubmitTimedOutButQueued(t *testing.T) {
	f := newFixture(t)
	dir := f.namedPodDir("ns-uid1", "uid1")
	f.remote.failing = []submitFail{{err: timedOut("/compute/sys/jobs"), queued: true}}
	id := f.submit(dir)
	if f.remote.submits != 1 || len(f.remote.jobs) != 1 || f.remote.lookups != 1 {
		t.Fatalf("%d submissions, %d jobs, %d lookups; want 1, 1, 1", f.remote.submits, len(f.remote.jobs), f.remote.lookups)
	}
	if f.remote.jobs[id] == nil || f.b.jobs[dir] == nil || f.b.jobs[dir].id != id {
		t.Fatalf("job %s not reported and tracked: %+v", id, f.b.jobs)
	}
}

// The last try failed too, but queued the job: that job is the answer.
func TestSubmitLastTryQueued(t *testing.T) {
	f := newFixture(t)
	dir := f.namedPodDir("ns-uid1", "uid1")
	for i := range f.b.retry.Attempts {
		f.remote.failing = append(f.remote.failing, submitFail{err: timedOut("/compute/sys/jobs"), queued: i == f.b.retry.Attempts-1})
	}
	f.submit(dir)
	if f.remote.submits != 4 || f.remote.lookups != 4 || len(f.remote.jobs) != 1 {
		t.Fatalf("%d submissions, %d lookups, %d jobs; want 4, 4, 1", f.remote.submits, f.remote.lookups, len(f.remote.jobs))
	}

	// Given up without a job, the bridge says so.
	f.remote.failing = []submitFail{{err: timedOut("/compute/sys/jobs")}, {err: timedOut("/compute/sys/jobs")}, {err: timedOut("/compute/sys/jobs")}, {err: timedOut("/compute/sys/jobs")}}
	f.remote.submits, f.remote.lookups = 0, 0
	other := f.namedPodDir("ns-uid2", "uid2")
	if res := f.b.Exec(context.Background(), "sbatch", []string{other + "/job.slurm"}); res.Code == 0 || f.remote.submits != 4 || f.remote.lookups != 4 {
		t.Fatalf("sbatch %+v after %d submissions and %d lookups; want a failure after 4 and 4", res, f.remote.submits, f.remote.lookups)
	}
}

// The same pod submitted again (retry in place keeps its UID and directory):
// an ended job of that name and directory, untracked since the directory was
// removed, is not the one a failed submission may have queued.
func TestSubmitIgnoresEndedJob(t *testing.T) {
	f := newFixture(t)
	dir := f.namedPodDir("ns-uid1", "uid1")
	f.remote.jobs["50"] = &firecrest.Job{JobID: "50", Name: "uid1", WorkingDirectory: dir, Status: firecrest.JobStatus{State: "CANCELLED by 1001"}}
	f.remote.failing = []submitFail{{err: timedOut("/compute/sys/jobs")}}
	if id := f.submit(dir); id == "50" || f.remote.submits != 2 {
		t.Fatalf("sbatch answered job %s after %d submissions; want a new job after 2", id, f.remote.submits)
	}
}

// Nor is a job the bridge submitted itself and stopped tracking when the
// plugin removed its directory, even one still running (a failed scancel).
func TestSubmitIgnoresForgottenJob(t *testing.T) {
	f := newFixture(t)
	dir := f.namedPodDir("ns-uid1", "uid1")
	old := f.submit(dir)
	f.remote.setState(old, "RUNNING", 0, 0)
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	f.b.SyncAll(context.Background())
	if len(f.b.jobs) != 0 {
		t.Fatalf("job still tracked after its directory went: %+v", f.b.jobs)
	}

	dir = f.namedPodDir("ns-uid1", "uid1")
	f.remote.submits = 0
	f.remote.failing = []submitFail{{err: timedOut("/compute/sys/jobs")}}
	if id := f.submit(dir); id == old || f.remote.submits != 2 {
		t.Fatalf("sbatch answered job %s (the forgotten one is %s) after %d submissions; want a new job after 2", id, old, f.remote.submits)
	}
}

// A submission that failed without a job is sent again, once the bridge has
// looked; jobs of other names, other directories or that it tracks are not its.
func TestSubmitFailedIsRetried(t *testing.T) {
	f := newFixture(t)
	dir := f.namedPodDir("ns-uid1", "uid1")
	first := f.submit(dir) // tracked: the plugin submitting the same directory again
	other := f.namedPodDir("ns-uid2", "uid2")
	f.remote.jobs["90"] = &firecrest.Job{JobID: "90", Name: "uid1", WorkingDirectory: other}
	f.remote.jobs["91"] = &firecrest.Job{JobID: "91", Name: "uid2", WorkingDirectory: dir}

	f.remote.submits = 0
	f.remote.failing = []submitFail{
		{err: &firecrest.APIError{Method: "POST", Path: "/compute/sys/jobs", Status: http.StatusBadGateway}},
		{err: timedOut("/compute/sys/jobs")},
	}
	id := f.submit(dir)
	if id == first || id == "90" || id == "91" {
		t.Fatalf("took job %s, which is not the new submission's", id)
	}
	if f.remote.submits != 3 || f.remote.lookups != 2 {
		t.Fatalf("%d submissions, %d lookups; want 3 and 2", f.remote.submits, f.remote.lookups)
	}
}

// Unless FirecREST says no job was queued, none is submitted again.
func TestSubmitNotRetried(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		lookupErr error
		submits   int
		lookups   int
	}{
		{"lookup failed", timedOut("/compute/sys/jobs"), timedOut("/compute/sys/jobs"), 1, 1},
		{"4xx", &firecrest.APIError{Method: "POST", Path: "/compute/sys/jobs", Status: http.StatusBadRequest, Body: `{"message":"bad"}`}, nil, 1, 0},
		{"500", &firecrest.APIError{Method: "POST", Path: "/compute/sys/jobs", Status: http.StatusInternalServerError}, nil, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			dir := f.namedPodDir("ns-uid1", "uid1")
			f.remote.failing = []submitFail{{err: tc.err}}
			f.remote.jobsErr = tc.lookupErr
			res := f.b.Exec(context.Background(), "sbatch", []string{dir + "/job.slurm"})
			if res.Code == 0 {
				t.Fatalf("sbatch succeeded: %+v", res)
			}
			if f.remote.submits != tc.submits || f.remote.lookups != tc.lookups {
				t.Fatalf("%d submissions, %d lookups; want %d and %d", f.remote.submits, f.remote.lookups, tc.submits, tc.lookups)
			}
			if tc.lookupErr != nil && !strings.Contains(res.Stderr, "whether FirecREST queued the job is unknown") {
				t.Errorf("stderr %q does not say the job may exist", res.Stderr)
			}
		})
	}
}

// A submission that never reached FirecREST is sent again without a lookup.
func TestSubmitNotSentIsRetried(t *testing.T) {
	f := newFixture(t)
	dir := f.namedPodDir("ns-uid1", "uid1")
	refused := &url.Error{Op: "Post", URL: "https://f7t/compute/sys/jobs", Err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}}
	f.remote.failing = []submitFail{{err: refused}}
	f.submit(dir)
	if f.remote.submits != 2 || f.remote.lookups != 0 {
		t.Fatalf("%d submissions, %d lookups; want 2 and 0", f.remote.submits, f.remote.lookups)
	}
}

func TestJobName(t *testing.T) {
	for _, tc := range []struct{ text, want string }{
		{"#!/bin/bash\n#SBATCH --job-name=uid1\n#SBATCH --output=/r/job.out\n/r/job.sh\n", "uid1"},
		{"#!/bin/bash\n#SBATCH --job-name=uid1\n#SBATCH -J user-name\n\n#SBATCH --time=10\n/r/job.sh\n#SBATCH -J after\n", "user-name"},
		{"#!/bin/bash\n#SBATCH --job-name gang-a\n", "gang-a"},
		{"#!/bin/bash\n#SBATCH -Jx\n", "x"},
		{"#!/bin/bash\n# a comment\n/r/job.sh\n", "job.slurm"},
	} {
		if got := jobName("/r/job.slurm", tc.text); got != tc.want {
			t.Errorf("jobName(%q) = %q, want %q", tc.text, got, tc.want)
		}
	}
}

// The failure seen at a site: one upload of a job directory timed out in
// FirecREST, which failed the sbatch. Through the real client it is retried.
func TestSbatchSurvivesUploadTimeout(t *testing.T) {
	var uploads, failed atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/filesystem/sys/ops/upload":
			if uploads.Add(1) == 1 {
				failed.Add(1)
				w.WriteHeader(http.StatusGatewayTimeout)
				io.WriteString(w, `{"errorType":"error","message":"Command execution timeout limit exceeded."}`)
				return
			}
			w.WriteHeader(http.StatusCreated)
		case r.URL.Path == "/filesystem/sys/ops/ls":
			io.WriteString(w, `{"output":[]}`)
		case r.URL.Path == "/compute/sys/jobs" && r.Method == http.MethodPost:
			w.WriteHeader(http.StatusCreated)
			io.WriteString(w, `{"jobId":7}`)
		default: // mkdir, chmod
			io.WriteString(w, `{}`)
		}
	}))
	t.Cleanup(srv.Close)
	var log bytes.Buffer
	client, err := firecrest.New(context.Background(), firecrest.Options{
		URL: srv.URL, System: "sys", APIKey: "k",
		Retry: firecrest.Retry{Attempts: 4, Delay: time.Millisecond, MaxElapsed: 5 * time.Second, Log: slog.New(slog.NewTextHandler(&log, nil))},
	})
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t)
	f.b.remote = client
	dir := f.podDir("ns-uid1")
	res := f.b.Exec(context.Background(), "sbatch", []string{dir + "/job.slurm"})
	if res.Code != 0 || res.Stdout != "Submitted batch job 7\n" {
		t.Fatalf("sbatch after one upload timed out: %+v", res)
	}
	if failed.Load() != 1 || !strings.Contains(log.String(), "retrying") {
		t.Fatalf("%d failed uploads, log %q; want 1 and its retry", failed.Load(), log.String())
	}
}
