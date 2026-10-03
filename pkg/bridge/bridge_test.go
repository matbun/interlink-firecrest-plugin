package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// Copied from interlink-slurm-plugin pkg/slurm (Status.go, prepare.go at
// 0.6.3-pre2): the shim output has to keep matching what the plugin parses.
const (
	pluginStatePattern    = `(CD|CG|F|OOM|PD|PR|R|ST|S|TO)`
	pluginExitCodePattern = `([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])\s`
	pluginJIDPattern      = `Submitted batch job (?P<jid>\d+)`
)

type fixture struct {
	t      *testing.T
	root   string
	remote *fakeRemote
	b      *Bridge
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	root := filepath.Join(t.TempDir(), "jobs")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		FirecrestURL: "http://f7t", System: "sys", APIKey: "k", JobRoot: root,
	}
	cfg.applyDefaults()
	remote := newFakeRemote()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &fixture{t: t, root: root, remote: remote, b: New(cfg, remote, log)}
}

// podDir creates a job directory the way the plugin's Create leaves it before
// calling sbatch.
func (f *fixture) podDir(name string) string {
	f.t.Helper()
	dir := filepath.Join(f.root, name)
	for path, content := range map[string]string{
		"job.slurm":                     "#!/bin/bash\n#SBATCH --output=" + dir + "/job.out\n" + dir + "/job.sh\n",
		"job.sh":                        "#!/bin/bash\necho hi\n",
		"ctn_envfile.properties":        "A=1\n",
		"configMaps/cfg/settings.yaml":  "x: 1\n",
		"emptyDirs/scratch/.keep-empty": "",
	} {
		p := filepath.Join(dir, path)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			f.t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			f.t.Fatal(err)
		}
	}
	os.Remove(filepath.Join(dir, "emptyDirs/scratch/.keep-empty"))
	os.Chmod(filepath.Join(dir, "job.sh"), 0o774)
	os.Chmod(filepath.Join(dir, "job.slurm"), 0o774)
	return dir
}

func (f *fixture) submit(dir string) string {
	f.t.Helper()
	res := f.b.Exec(context.Background(), "sbatch", []string{dir + "/job.slurm"})
	if res.Code != 0 {
		f.t.Fatalf("sbatch failed: %+v", res)
	}
	m := regexp.MustCompile(pluginJIDPattern).FindStringSubmatch(res.Stdout)
	if m == nil {
		f.t.Fatalf("plugin cannot parse sbatch output %q", res.Stdout)
	}
	return m[1]
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestSbatchStagesDirectoryAndSubmitsScript(t *testing.T) {
	f := newFixture(t)
	dir := f.podDir("ns-uid1")
	id := f.submit(dir)
	if id != "1" {
		t.Fatalf("job id %q", id)
	}

	for _, rel := range []string{"job.slurm", "job.sh", "ctn_envfile.properties", "configMaps/cfg/settings.yaml"} {
		got, ok := f.remote.read(filepath.Join(dir, rel))
		if !ok || got != readFile(t, filepath.Join(dir, rel)) {
			t.Errorf("remote %s = %q, %v", rel, got, ok)
		}
	}
	if !f.remote.dirs[filepath.Join(dir, "emptyDirs/scratch")] {
		t.Error("empty directory not created remotely")
	}
	if mode := f.remote.files[filepath.Join(dir, "job.sh")].mode; mode != "774" {
		t.Errorf("job.sh mode %s, want 774", mode)
	}
	if mode := f.remote.files[filepath.Join(dir, "ctn_envfile.properties")].mode; mode != "644" {
		t.Errorf("envfile mode %s, want 644", mode)
	}
	spec := f.remote.specs[0]
	if spec.ScriptPath != dir+"/job.slurm" || spec.WorkingDirectory != dir {
		t.Errorf("spec %+v", spec)
	}
}

func TestSbatchRefusesScriptOutsideJobRoot(t *testing.T) {
	f := newFixture(t)
	for _, p := range []string{"/etc/job.slurm", f.root + "/job.slurm", f.root + "/../x/job.slurm", "relative/job.slurm"} {
		res := f.b.Exec(context.Background(), "sbatch", []string{p})
		if res.Code == 0 || res.Stderr == "" {
			t.Errorf("sbatch %s accepted: %+v", p, res)
		}
	}
	if len(f.remote.specs) != 0 {
		t.Fatal("a job was submitted")
	}
}

// The expected lines are what slurm-wlm 24.11.3 printed for the same command
// (LOG 2026-10-03): two fields, each padded to 20 columns.
func TestSqueueMatchesSlurmAndPluginParsing(t *testing.T) {
	f := newFixture(t)
	id := f.submit(f.podDir("ns-uid1"))
	args := []string{"--noheader", "-a", "--states=all", "-O", "exit_code,StateCompact", "-j", id}

	for _, tc := range []struct {
		state        string
		exit, signal int
		line         string
		pluginState  string
	}{
		{"PENDING", 0, 0, "0:0                 PD                  \n", "PD"},
		{"RUNNING", 0, 0, "0:0                 R                   \n", "R"},
		{"COMPLETED", 0, 0, "0:0                 CD                  \n", "CD"},
		{"FAILED", 3, 0, "3:0                 F                   \n", "F"},
		{"CANCELLED by 1001", 0, 15, "0:15                CA                  \n", ""},
		{"OUT_OF_MEMORY", 0, 125, "0:125               OOM                 \n", "OOM"},
		{"TIMEOUT", 0, 0, "0:0                 TO                  \n", "TO"},
	} {
		f.remote.setState(id, tc.state, tc.exit, tc.signal)
		res := f.b.Exec(context.Background(), "squeue", args)
		if res.Code != 0 || res.Stderr != "" {
			t.Fatalf("%s: %+v", tc.state, res)
		}
		if res.Stdout != tc.line {
			t.Errorf("%s: got %q, want %q", tc.state, res.Stdout, tc.line)
		}
		if got := regexp.MustCompile(pluginStatePattern).FindString(res.Stdout); got != tc.pluginState {
			t.Errorf("%s: plugin reads state %q, want %q", tc.state, got, tc.pluginState)
		}
		// The plugin indexes the exit code match without a check; no match panics.
		if regexp.MustCompile(pluginExitCodePattern).FindStringSubmatch(res.Stdout) == nil {
			t.Errorf("%s: plugin exit code regex does not match %q", tc.state, res.Stdout)
		}
	}
}

func TestSqueueUnknownJobFailsLikeSlurm(t *testing.T) {
	f := newFixture(t)
	res := f.b.Exec(context.Background(), "squeue", []string{"--noheader", "-a", "--states=all", "-O", "exit_code,StateCompact", "-j", "99999"})
	if res.Code != 1 || res.Stderr != "slurm_load_jobs error: Invalid job id specified\n" || res.Stdout != "" {
		t.Fatalf("got %+v", res)
	}
}

func TestSqueueMeIsACachedPing(t *testing.T) {
	f := newFixture(t)
	for i := 0; i < 3; i++ {
		if res := f.b.Exec(context.Background(), "squeue", []string{"--me"}); res.Code != 0 || res.Stderr != "" {
			t.Fatalf("got %+v", res)
		}
	}
	if f.remote.pings != 1 {
		t.Fatalf("%d FirecREST pings, want 1 within PingTTL", f.remote.pings)
	}
}

func TestTerminalStatePullsStatusFilesFirst(t *testing.T) {
	f := newFixture(t)
	dir := f.podDir("ns-uid1")
	id := f.submit(dir)
	f.remote.write(dir+"/job.out", "hello\n")
	f.remote.write(dir+"/run-ctn.out", "from the container\n")
	f.remote.write(dir+"/run-ctn.status", "3\n")
	f.remote.write(dir+"/compute-node", "nid001\n")
	f.remote.setState(id, "FAILED", 3, 0)

	res := f.b.Exec(context.Background(), "squeue", []string{"--noheader", "-O", "exit_code,StateCompact", "-j", id})
	if res.Code != 0 {
		t.Fatal(res.Stderr)
	}
	for rel, want := range map[string]string{
		"run-ctn.status": "3\n", "run-ctn.out": "from the container\n", "job.out": "hello\n", "compute-node": "nid001\n",
	} {
		if got := readFile(t, filepath.Join(dir, rel)); got != want {
			t.Errorf("%s = %q, want %q", rel, got, want)
		}
	}
}

func TestPullFetchesLogTailsAndRewritesSmallFiles(t *testing.T) {
	f := newFixture(t)
	dir := f.podDir("ns-uid1")
	f.submit(dir)
	log := strings.Repeat("x", smallFile+10)
	f.remote.write(dir+"/run-ctn.out", log)
	f.remote.write(dir+"/probe-ctn.status", "0\n")
	f.b.SyncAll(context.Background())

	// The plugin follows logs through an open descriptor: keep the same inode.
	fd, err := os.Open(filepath.Join(dir, "run-ctn.out"))
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()
	before, _ := fd.Stat()

	f.remote.views = nil
	f.remote.write(dir+"/run-ctn.out", log+"tail\n")
	f.remote.write(dir+"/probe-ctn.status", "127\n")
	f.b.SyncAll(context.Background())

	if got := readFile(t, filepath.Join(dir, "run-ctn.out")); got != log+"tail\n" {
		t.Errorf("log has %d bytes, want %d", len(got), len(log)+5)
	}
	if got := readFile(t, filepath.Join(dir, "probe-ctn.status")); got != "127\n" {
		t.Errorf("status = %q, want %q", got, "127\n")
	}
	after, _ := os.Stat(filepath.Join(dir, "run-ctn.out"))
	if !os.SameFile(before, after) {
		t.Error("log was replaced instead of written in place")
	}
	for _, v := range f.remote.views {
		if strings.HasSuffix(v.path, "run-ctn.out") && v.offset != int64(len(log)) {
			t.Errorf("log fetched from offset %d, want only the tail from %d", v.offset, len(log))
		}
	}

	// Unchanged files are not fetched again.
	f.remote.views = nil
	f.b.SyncAll(context.Background())
	if len(f.remote.views) != 0 {
		t.Errorf("%d views for unchanged files", len(f.remote.views))
	}
}

func TestShrunkFileIsRefetchedWhole(t *testing.T) {
	f := newFixture(t)
	dir := f.podDir("ns-uid1")
	f.submit(dir)
	f.remote.write(dir+"/run-ctn.out", strings.Repeat("a", smallFile+100))
	f.b.SyncAll(context.Background())
	f.remote.write(dir+"/run-ctn.out", strings.Repeat("b", smallFile+50))
	f.b.SyncAll(context.Background())
	if got := readFile(t, filepath.Join(dir, "run-ctn.out")); got != strings.Repeat("b", smallFile+50) {
		t.Errorf("got %d bytes starting %q", len(got), got[:1])
	}
}

func TestDeletedPodDirIsRemovedRemotely(t *testing.T) {
	f := newFixture(t)
	dir := f.podDir("ns-uid1")
	keep := f.podDir("ns-uid2")
	f.submit(dir)
	f.submit(keep)
	os.RemoveAll(dir)
	f.b.SyncAll(context.Background())
	if len(f.remote.removed) != 1 || f.remote.removed[0] != dir {
		t.Fatalf("removed %v, want [%s]", f.remote.removed, dir)
	}
	if _, ok := f.remote.read(keep + "/job.slurm"); !ok {
		t.Fatal("other job directory removed")
	}
	if len(f.b.snapshot()) != 1 {
		t.Fatal("deleted job still tracked")
	}
}

func TestMissingJobRootRemovesNothing(t *testing.T) {
	f := newFixture(t)
	f.submit(f.podDir("ns-uid1"))
	os.RemoveAll(f.root)
	f.b.SyncAll(context.Background())
	if len(f.remote.removed) != 0 {
		t.Fatalf("removed %v with the job root missing", f.remote.removed)
	}
}

func TestRecoverPicksUpPluginJobs(t *testing.T) {
	f := newFixture(t)
	dir := f.podDir("ns-uid1")
	id := f.submit(dir)
	os.WriteFile(filepath.Join(dir, "JobID.jid"), []byte(id), 0o644)

	restarted := New(f.b.cfg, f.remote, f.b.log)
	if err := restarted.Recover(); err != nil {
		t.Fatal(err)
	}
	f.remote.write(dir+"/run-ctn.status", "0\n")
	f.remote.setState(id, "COMPLETED", 0, 0)
	res := restarted.Exec(context.Background(), "squeue", []string{"--noheader", "-O", "exit_code,StateCompact", "-j", id})
	if res.Code != 0 {
		t.Fatal(res.Stderr)
	}
	if got := readFile(t, filepath.Join(dir, "run-ctn.status")); got != "0\n" {
		t.Fatalf("status = %q after restart", got)
	}
	if got := readFile(t, filepath.Join(dir, "JobID.jid")); got != id {
		t.Fatalf("plugin-only file overwritten: %q", got)
	}
}

func TestScancelUnknownJobIsSilent(t *testing.T) {
	f := newFixture(t)
	if res := f.b.Exec(context.Background(), "scancel", []string{"99999"}); res.Code != 0 || res.Stderr != "" || res.Stdout != "" {
		t.Fatalf("got %+v", res)
	}
	id := f.submit(f.podDir("ns-uid1"))
	if res := f.b.Exec(context.Background(), "scancel", []string{id}); res.Code != 0 {
		t.Fatalf("got %+v", res)
	}
	if j, _ := f.remote.GetJob(context.Background(), id); !strings.HasPrefix(j.Status.State, "CANCELLED") {
		t.Fatalf("state %q", j.Status.State)
	}
}

func TestSinfoFormats(t *testing.T) {
	f := newFixture(t)

	res := f.b.Exec(context.Background(), "sinfo", []string{"--json"})
	var parsed struct {
		Nodes []struct {
			CPUs        int64 `json:"cpus"`
			AllocCPUs   int64 `json:"alloc_cpus"`
			RealMemory  int64 `json:"real_memory"`
			FreeMemory  int64 `json:"free_memory"`
			AllocMemory int64 `json:"alloc_memory"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal([]byte(res.Stdout), &parsed); err != nil || len(parsed.Nodes) != 1 {
		t.Fatalf("sinfo --json %q: %v", res.Stdout, err)
	}
	if n := parsed.Nodes[0]; n.CPUs != 2 || n.AllocCPUs != 1 || n.RealMemory != 16650 || n.AllocMemory != 1000 {
		t.Errorf("node %+v", n)
	}

	res = f.b.Exec(context.Background(), "sinfo", []string{"--noheader", "-N", "--format=%N,%c,%m,%e"})
	if want := "nid001,2,16650,15650\nnid001,2,16650,15650\n"; res.Stdout != want {
		t.Errorf("sinfo -N = %q, want %q", res.Stdout, want)
	}

	res = f.b.Exec(context.Background(), "sinfo", []string{"-s"})
	if !strings.HasPrefix(res.Stdout, "PARTITION AVAIL  TIMELIMIT   NODES(A/I/O/T) NODELIST\n") ||
		!strings.Contains(res.Stdout, "normal       up        n/a          1/0/0/1 nid001") {
		t.Errorf("sinfo -s = %q", res.Stdout)
	}

	if res := f.b.Exec(context.Background(), "sinfo", []string{"-R"}); res.Code == 0 {
		t.Error("unsupported option accepted")
	}
}

func TestShimRoundTripOverSocket(t *testing.T) {
	f := newFixture(t)
	socket := filepath.Join(t.TempDir(), "b.sock")
	l, err := Listen(socket)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: f.b.Handler()}
	go srv.Serve(l)
	defer srv.Close()

	dir := f.podDir("ns-uid1")
	var out, errOut bytes.Buffer
	if code := RunShim("sbatch", []string{dir + "/job.slurm"}, socket, &out, &errOut); code != 0 {
		t.Fatalf("code %d, stderr %q", code, errOut.String())
	}
	if out.String() != "Submitted batch job 1\n" || errOut.Len() != 0 {
		t.Fatalf("stdout %q stderr %q", out.String(), errOut.String())
	}

	out.Reset()
	if code := RunShim("squeue", []string{"-j", "42", "-O", "StateCompact"}, socket, &out, &errOut); code != 1 {
		t.Fatalf("unknown job: code %d", code)
	}
	if errOut.String() != "slurm_load_jobs error: Invalid job id specified\n" {
		t.Fatalf("stderr %q", errOut.String())
	}

	errOut.Reset()
	if code := RunShim("squeue", []string{"--me"}, filepath.Join(t.TempDir(), "none.sock"), &out, &errOut); code != 1 || errOut.Len() == 0 {
		t.Fatal("an unreachable daemon must fail with a message on stderr")
	}
}

func TestSyncLoopStops(t *testing.T) {
	f := newFixture(t)
	f.b.cfg.SyncInterval = Duration(10 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.b.Run(ctx); close(done) }()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
