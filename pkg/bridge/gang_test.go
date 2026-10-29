package bridge

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// gang creates a head and a member directory and rewrites the head's job.slurm
// the way the plugin's gang build renders the per-rank launch: one srun per
// member, each running the member's own job.sh with its log in its own
// directory.
func (f *fixture) gang() (head, member string) {
	f.t.Helper()
	return f.gangIn("")
}

// gangIn does the same below base, a job-workdir annotation's directory.
func (f *fixture) gangIn(base string) (head, member string) {
	f.t.Helper()
	head, member = f.podDir(filepath.Join(base, "ns-head")), f.podDir(filepath.Join(base, "ns-member"))
	script := "#!/bin/bash\n#SBATCH --nodes=2\n" +
		"mkdir -p " + f.root + "/.gang-heads && hostname -f > " + f.root + "/.gang-heads/ns-g || true\n"
	for i, d := range []string{head, member} {
		script += "srun --overlap --nodes=1 --ntasks=1 -w \"${nodes[" + string(rune('0'+i)) + "]}\" \\\n" +
			"     -o " + d + "/job.out -e " + d + "/job.err --open-mode=truncate \\\n" +
			"  bash -c 'hostname -f > " + d + "/compute-node || true\nexec " + d + "/job.sh' &\n"
	}
	if err := os.WriteFile(filepath.Join(head, "job.slurm"), []byte(script), 0o600); err != nil {
		f.t.Fatal(err)
	}
	return head, member
}

func (f *fixture) dirsOf(id string) []string {
	var dirs []string
	for _, j := range f.b.lookupAll(id) {
		dirs = append(dirs, j.dir)
	}
	sort.Strings(dirs)
	return dirs
}

// expire ends the sync interval of a job's shared FirecREST answer.
func (f *fixture) expire(id string) {
	if q := f.b.queries[id]; q != nil {
		q.at = time.Time{}
	}
}

// settle runs enough sync rounds for every remaining directory of an ended
// job to get its final pulls, and for the directories nothing needs any more
// to be removed.
func (f *fixture) settle() {
	for range f.b.cfg.FinalSyncRounds + 4 {
		f.b.SyncAll(context.Background())
	}
}

func (f *fixture) squeue(id string) {
	f.t.Helper()
	if res := f.b.Exec(context.Background(), "squeue", []string{"--noheader", "-O", "exit_code,StateCompact", "-j", id}); res.Code != 0 {
		f.t.Fatal(res.Stderr)
	}
}

func TestGangSbatchStagesEveryMemberDirectory(t *testing.T) {
	f := newFixture(t)
	head, member := f.gang()
	other := f.podDir("ns-other")
	// Named, but not job directories of this gang.
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(f.root, "ns-link")); err != nil {
		t.Fatal(err)
	}
	extra := "cat /x" + other + "/job.sh " + f.root + "/../" + filepath.Base(other) + "/job.sh " +
		f.root + "/ns-link/x " + f.root + "/ns-missing/x\n"
	script := readFile(t, filepath.Join(head, "job.slurm")) + extra
	os.WriteFile(filepath.Join(head, "job.slurm"), []byte(script), 0o600)

	id := f.submit(head)

	for _, p := range []string{head + "/job.slurm", head + "/job.sh", member + "/job.sh", member + "/configMaps/cfg/settings.yaml"} {
		if _, ok := f.remote.read(p); !ok {
			t.Errorf("%s not staged", p)
		}
	}
	for _, p := range []string{other + "/job.sh", f.root + "/.gang-heads", f.root + "/ns-missing"} {
		if _, ok := f.remote.read(p); ok || f.remote.dirs[p] {
			t.Errorf("%s staged", p)
		}
	}
	if got := f.dirsOf(id); len(got) != 2 || got[0] != head || got[1] != member {
		t.Fatalf("job %s tracks %v, want [%s %s]", id, got, head, member)
	}
	if spec := f.remote.specs[0]; spec.ScriptPath != head+"/job.slurm" || spec.WorkingDirectory != head {
		t.Errorf("spec %+v", spec)
	}
}

func TestGangStatusAndPullsCoverEveryDirectory(t *testing.T) {
	f := newFixture(t)
	head, member := f.gang()
	id := f.submit(head)

	// Pending: no directory is pulled.
	f.squeue(id)
	f.remote.write(member+"/job.out", "early\n")
	f.b.SyncAll(context.Background())
	if _, err := os.Stat(member + "/job.out"); err == nil {
		t.Fatal("pending gang was pulled")
	}

	f.remote.setState(id, "RUNNING", 0, 0)
	f.expire(id)
	f.squeue(id)
	f.b.SyncAll(context.Background())
	if got := readFile(t, member+"/job.out"); got != "early\n" {
		t.Fatalf("member log %q", got)
	}

	// Written just before the end: the final sync before the terminal state
	// picks them up in every directory.
	f.remote.write(head+"/run-ctn.status", "0\n")
	f.remote.write(member+"/run-ctn.status", "3\n")
	f.remote.write(member+"/compute-node", "nid002\n")
	f.remote.setState(id, "FAILED", 3, 0)
	f.expire(id)
	f.squeue(id)
	for p, want := range map[string]string{head + "/run-ctn.status": "0\n", member + "/run-ctn.status": "3\n", member + "/compute-node": "nid002\n"} {
		if got := readFile(t, p); got != want {
			t.Errorf("%s = %q, want %q", p, got, want)
		}
	}
	for _, j := range f.b.lookupAll(id) {
		if !j.terminal || j.state != "F" {
			t.Errorf("%s: terminal %v state %q", j.dir, j.terminal, j.state)
		}
	}
}

// A site launcher writes every rank's log in the head's directory and links it
// from the member's.
func TestGangPullsLinkedLogs(t *testing.T) {
	f := newFixture(t)
	head, member := f.gang()
	id := f.submit(head)
	f.running(id)
	f.remote.write(head+"/ranks/rank-1.out", "rank 1\n")
	f.remote.link(member+"/run-ctn.out", head+"/ranks/rank-1.out")
	f.remote.write(member+"/run-ctn.status", "0\n")
	f.b.SyncAll(context.Background())
	if got := readFile(t, member+"/run-ctn.out"); got != "rank 1\n" {
		t.Fatalf("linked log %q", got)
	}

	// A link whose target does not exist yet fails a dereferencing ls; the
	// other files still come back.
	f.remote.link(member+"/run-late.out", head+"/ranks/rank-9.out")
	f.remote.write(member+"/run-ctn.status", "1\n")
	f.b.SyncAll(context.Background())
	if got := readFile(t, member+"/run-ctn.status"); got != "1\n" {
		t.Fatalf("status %q with a dangling link", got)
	}
	if _, err := os.Stat(member + "/run-late.out"); err == nil {
		t.Fatal("dangling link pulled")
	}
}

func TestGangRemoteCleanupWaitsForTheJob(t *testing.T) {
	f := newFixture(t)
	head, member := f.gang()
	id := f.submit(head)
	f.running(id)

	// The head pod is deleted first while the member's rank still runs.
	os.RemoveAll(head)
	f.b.SyncAll(context.Background())
	if len(f.remote.removed) != 0 {
		t.Fatalf("removed %v while the job runs", f.remote.removed)
	}
	f.remote.setState(id, "COMPLETED", 0, 0)
	f.expire(id)
	f.squeue(id)
	if _, err := os.Stat(head); err == nil {
		t.Fatal("final sync recreated the removed head directory")
	}
	f.b.SyncAll(context.Background())
	if len(f.remote.removed) != 0 {
		t.Fatalf("removed %v before the member's final pulls", f.remote.removed)
	}
	f.settle()
	if len(f.remote.removed) != 1 || f.remote.removed[0] != head {
		t.Fatalf("removed %v, want [%s] once the job ended", f.remote.removed, head)
	}
	if _, ok := f.remote.read(member + "/job.sh"); !ok {
		t.Fatal("member directory removed with the head")
	}

	os.RemoveAll(member)
	f.b.SyncAll(context.Background())
	if len(f.remote.removed) != 2 || len(f.b.snapshot()) != 0 {
		t.Fatalf("removed %v, %d jobs tracked", f.remote.removed, len(f.b.snapshot()))
	}
	if len(f.b.queries) != 0 {
		t.Fatal("query cache kept for a forgotten job")
	}
}

// The plugin cancels a gang when its last member is deleted: then nothing is
// left to wait for.
func TestGangAllMembersDeletedRemovesEverything(t *testing.T) {
	f := newFixture(t)
	head, member := f.gang()
	f.running(f.submit(head))
	os.RemoveAll(head)
	os.RemoveAll(member)
	f.b.SyncAll(context.Background())
	if len(f.remote.removed) != 2 {
		t.Fatalf("removed %v", f.remote.removed)
	}
}

func TestGangSqueueSharesOneGetJobPerInterval(t *testing.T) {
	f := newFixture(t)
	head, _ := f.gang()
	id := f.submit(head)
	single := f.submit(f.podDir("ns-single"))

	f.remote.gets = 0
	for i := 0; i < 4; i++ {
		f.squeue(id)
	}
	if f.remote.gets != 1 {
		t.Fatalf("%d GetJob calls for one gang in one interval, want 1", f.remote.gets)
	}
	f.b.cfg.SyncInterval = Duration(time.Nanosecond)
	f.squeue(id)
	if f.remote.gets != 2 {
		t.Fatalf("%d GetJob calls after the interval, want 2", f.remote.gets)
	}

	// A single pod's job is asked every time, as before.
	f.b.cfg.SyncInterval = Duration(time.Hour)
	f.remote.gets = 0
	f.squeue(single)
	f.squeue(single)
	if f.remote.gets != 2 {
		t.Fatalf("%d GetJob calls for two squeue of a single pod, want 2", f.remote.gets)
	}
}

// The plugin writes the gang's job id into every member's directory.
func TestRecoverRebuildsGangDirectories(t *testing.T) {
	f := newFixture(t)
	head, member := f.gang()
	id := f.submit(head)
	for _, d := range []string{head, member} {
		os.WriteFile(filepath.Join(d, "JobID.jid"), []byte(id), 0o600)
	}

	restarted := New(f.b.cfg, f.remote, f.b.log)
	if err := restarted.Recover(); err != nil {
		t.Fatal(err)
	}
	f.b = restarted
	if got := f.dirsOf(id); len(got) != 2 {
		t.Fatalf("recovered %v", got)
	}

	// Not polled yet after the restart: the job may still run.
	os.RemoveAll(head)
	f.b.SyncAll(context.Background())
	if len(f.remote.removed) != 0 {
		t.Fatalf("removed %v before the job state is known", f.remote.removed)
	}

	f.remote.write(member+"/run-ctn.status", "0\n")
	f.remote.setState(id, "COMPLETED", 0, 0)
	f.expire(id)
	f.squeue(id)
	if got := readFile(t, member+"/run-ctn.status"); got != "0\n" {
		t.Fatalf("member status %q after restart", got)
	}
	f.settle()
	if len(f.remote.removed) != 1 || f.remote.removed[0] != head {
		t.Fatalf("removed %v", f.remote.removed)
	}
}

// With a job-workdir annotation the plugin writes job.slurm in <base>/<ns>-<uid>:
// the base is not a job directory, and a pod's siblings are not either unless
// the script names them.
func TestJobWorkdirStagesOnlyPodDirectories(t *testing.T) {
	f := newFixture(t)
	base := filepath.Join(f.root, "wd")
	other := f.podDir("wd/ns-other")
	id := f.submit(f.podDir("wd/ns-single"))
	if got := f.dirsOf(id); len(got) != 1 || got[0] != base+"/ns-single" {
		t.Fatalf("single pod tracks %v", got)
	}
	if _, ok := f.remote.read(other + "/job.sh"); ok {
		t.Fatal("sibling pod staged with a single pod")
	}

	// A gang below the base stages its members, and a directory the script
	// names that holds no job.sh is not one of them.
	head, member := f.gangIn("wd")
	data := filepath.Join(base, "ns-data")
	os.MkdirAll(data, 0o755)
	script := readFile(t, filepath.Join(head, "job.slurm")) + "cat " + data + "/input\n"
	os.WriteFile(filepath.Join(head, "job.slurm"), []byte(script), 0o600)
	id = f.submit(head)
	if got := f.dirsOf(id); len(got) != 2 || got[0] != head || got[1] != member {
		t.Fatalf("gang tracks %v, want [%s %s]", got, head, member)
	}
	if _, ok := f.remote.read(other + "/job.sh"); ok {
		t.Fatal("sibling pod staged with the gang")
	}
}

// The plugin can remove a member's directory while its files are being
// listed; the pull must not bring it back, or the directory would keep the
// whole gang's remote copies alive.
func TestGangMemberRemovedDuringPullStaysRemoved(t *testing.T) {
	f := newFixture(t)
	head, member := f.gang()
	f.running(f.submit(head))
	f.remote.write(member+"/job.out", "rank 1\n")
	f.remote.onLs = func(path string) {
		if path == member {
			os.RemoveAll(member)
		}
	}
	f.b.SyncAll(context.Background())
	if _, err := os.Stat(member); err == nil {
		t.Fatal("pull recreated the removed member directory")
	}
}

// The plugin's last delete cancels the gang and nothing polls it afterwards:
// the bridge asks for the state itself before it keeps a removed directory.
func TestGangCancelledByLastDeleteIsCleanedUp(t *testing.T) {
	f := newFixture(t)
	head, member := f.gang()
	id := f.submit(head)
	f.running(id)

	// The member's directory is left behind; the head's goes with the cancel.
	if res := f.b.Exec(context.Background(), "scancel", []string{id}); res.Code != 0 {
		t.Fatal(res.Stderr)
	}
	os.RemoveAll(head)
	f.expire(id)
	f.settle()
	if len(f.remote.removed) != 1 || f.remote.removed[0] != head {
		t.Fatalf("removed %v, want [%s] once the job is cancelled", f.remote.removed, head)
	}
	if _, ok := f.remote.read(member + "/job.sh"); !ok {
		t.Fatal("remote copy of the remaining member removed")
	}
}

// A member's log in the launcher path is a link into the head's directory. The
// head pod may be deleted alone while the job runs on; the head's remote copy
// must outlive the member's final pull, or the lines written since the last
// regular pull, where a summary or a traceback lands, are lost.
func TestGangHeadKeptForMembersFinalPull(t *testing.T) {
	// Which directory a sync round visits first follows map order: repeat.
	for range 20 {
		f := newFixture(t)
		head, member := f.gang()
		id := f.submit(head)
		f.running(id)
		f.remote.write(head+"/ranks/rank-1.out", "line1\n")
		f.remote.link(member+"/run-ctn.out", head+"/ranks/rank-1.out")
		f.b.SyncAll(context.Background())

		os.RemoveAll(head)
		f.b.SyncAll(context.Background())
		f.remote.write(head+"/ranks/rank-1.out", "line1\nline2-last\n")
		f.remote.setState(id, "COMPLETED", 0, 0)
		f.expire(id)
		// The sync loop sees the end before the plugin polls again.
		f.b.SyncAll(context.Background())
		f.squeue(id)
		f.settle()

		if got := readFile(t, member+"/run-ctn.out"); got != "line1\nline2-last\n" {
			t.Fatalf("member final log %q, want the last line; removed %v", got, f.remote.removed)
		}
		if len(f.remote.removed) != 1 || f.remote.removed[0] != head {
			t.Fatalf("removed %v, want [%s] after the member's final pulls", f.remote.removed, head)
		}
	}
}
