package bridge

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newStage(t *testing.T, direction string, paths ...string) (Stage, *bytes.Buffer) {
	t.Helper()
	var out bytes.Buffer
	return Stage{
		Direction: direction, Paths: paths, Root: "/scratch/u/stage/ns", User: "fireuser",
		MaxFileSize: 1024, Parallelism: 2, Local: t.TempDir(), Out: &out,
	}, &out
}

func writeLocal(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

func TestStageFromEnv(t *testing.T) {
	keys := t.TempDir()
	writeLocal(t, keys+"/client-id", "cid\n", 0o400)
	writeLocal(t, keys+"/client-secret", "csecret\n", 0o400)
	env := map[string]string{
		"STAGE_DIRECTION": "in",
		"STAGE_PATHS":     "/shared/a/:/eos/b:/shared/a",
		"STAGE_ROOT":      "/scratch/u/stage",
		"STAGE_NAMESPACE": "ns",
		"REMOTE":          "fireuser@https://api.example.org/firecrest/v2?system=eiger&token=https://auth.example.org/realms/x/token",
		"SSH_KEY":         keys + "/client-secret",
	}
	s, opts, err := StageFromEnv(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(s.Paths, ":"); got != "/shared/a:/eos/b:/shared/a" || s.Root != "/scratch/u/stage/ns" || s.User != "fireuser" ||
		len(s.Private) != 1 || s.Private[0] != keys {
		t.Errorf("paths %s, root %s, user %s", got, s.Root, s.User)
	}
	if opts.URL != "https://api.example.org/firecrest/v2" || opts.System != "eiger" || opts.TokenURL != "https://auth.example.org/realms/x/token" ||
		opts.ClientID != "cid" || opts.ClientSecret != "csecret" {
		t.Errorf("options %+v", opts)
	}

	for k, v := range map[string]string{
		"STAGE_DIRECTION": "both",
		"STAGE_PATHS":     "relative/x",
		"STAGE_ROOT":      "/scratch/../x",
		"STAGE_NAMESPACE": "a/b",
		"REMOTE":          "fireuser@https://api.example.org/firecrest/v2?system=eiger",
	} {
		bad := map[string]string{}
		for k2, v2 := range env {
			bad[k2] = v2
		}
		bad[k] = v
		if _, _, err := StageFromEnv(func(k string) string { return bad[k] }); err == nil {
			t.Errorf("%s=%s: no error", k, v)
		}
	}
	for _, p := range []string{"/shared/../etc", "/shared//a", "/shared/it's", "/shared/./a", "/"} {
		if _, err := cleanStagePath(p); err == nil {
			t.Errorf("%s: not refused", p)
		}
	}
}

// A folder goes to <root>/<path> with its tree, executable bit and links; what
// the job writes comes back to the same path on the cluster side.
func TestStageRoundTrip(t *testing.T) {
	f := newFakeRemote()
	in, _ := newStage(t, "in", "/shared/demo/in")
	local := in.Local
	writeLocal(t, local+"/shared/demo/in/a.txt", "alpha", 0o644)
	writeLocal(t, local+"/shared/demo/in/sub/b.bin", "\x00\xff\x01binary", 0o644)
	writeLocal(t, local+"/shared/demo/in/run.sh", "#!/bin/sh\n", 0o755)
	if err := os.Mkdir(local+"/shared/demo/in/empty", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a.txt", local+"/shared/demo/in/link"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../a.txt", local+"/shared/demo/in/sub/up"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("sub", local+"/shared/demo/in/subdir"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("..", local+"/shared/demo/in/sub/loop"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing", local+"/shared/demo/in/dangling"); err != nil {
		t.Fatal(err)
	}
	if err := in.Run(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	// A second run replaces what the first copied.
	if err := in.Run(context.Background(), f); err != nil {
		t.Fatalf("second run: %v", err)
	}
	root := "/scratch/u/stage/ns/shared/demo/in"
	// Links are followed; a link back up and a link to nothing are skipped.
	for name, want := range map[string]string{"a.txt": "alpha", "sub/b.bin": "\x00\xff\x01binary", "run.sh": "#!/bin/sh\n",
		"link": "alpha", "sub/up": "alpha", "subdir/b.bin": "\x00\xff\x01binary", "subdir/up": "alpha"} {
		if got, ok := f.read(root + "/" + name); !ok || got != want {
			t.Errorf("%s: %q %v", name, got, ok)
		}
	}
	if f.files[root+"/run.sh"].mode != "755" || f.files[root+"/a.txt"].mode != "644" {
		t.Errorf("modes: run.sh %s, a.txt %s", f.files[root+"/run.sh"].mode, f.files[root+"/a.txt"].mode)
	}
	if !f.dirs[root+"/empty"] || f.dirs[root+"/sub/loop"] || f.files[root+"/dangling"] != nil || len(f.links) != 0 {
		t.Errorf("empty dir %v, loop %v, dangling %v, links %v", f.dirs[root+"/empty"], f.dirs[root+"/sub/loop"], f.files[root+"/dangling"], f.links)
	}

	// The job's outputs, as it writes them through its bind of the stage directory.
	outRoot := "/scratch/u/stage/ns/shared/demo/out"
	f.write(outRoot+"/result.txt", "done")
	f.write(outRoot+"/deep/x/y.bin", "\x00\x01")
	f.write(outRoot+"/tool", "#!/bin/sh\n")
	f.files[outRoot+"/tool"].mode = "755"
	f.link(outRoot+"/latest", "result.txt")
	out, buf := newStage(t, "out", "/shared/demo/out")
	out.Local = local
	writeLocal(t, local+"/shared/demo/out/result.txt", "stale", 0o644)
	if err := out.Run(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"result.txt": "done", "deep/x/y.bin": "\x00\x01", "tool": "#!/bin/sh\n", "latest": "done"} {
		if got, err := os.ReadFile(local + "/shared/demo/out/" + name); err != nil || string(got) != want {
			t.Errorf("%s: %q %v", name, got, err)
		}
	}
	if info, err := os.Stat(local + "/shared/demo/out/tool"); err != nil || info.Mode().Perm() != 0o755 {
		t.Errorf("tool: %v %v", info, err)
	}
	if target, err := os.Readlink(local + "/shared/demo/out/latest"); err != nil || target != "result.txt" {
		t.Errorf("latest: %q %v", target, err)
	}
	if !strings.Contains(buf.String(), "stage out: 3 files, 16 bytes, 1 links: done") {
		t.Errorf("summary: %s", buf.String())
	}
}

// A path that is a file, not a directory, is copied as that file both ways.
func TestStageSingleFile(t *testing.T) {
	f := newFakeRemote()
	in, _ := newStage(t, "in", "/shared/data.csv")
	writeLocal(t, in.Local+"/shared/data.csv", "1,2\n", 0o644)
	if err := in.Run(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	if got, ok := f.read("/scratch/u/stage/ns/shared/data.csv"); !ok || got != "1,2\n" {
		t.Errorf("staged in: %q %v", got, ok)
	}
	f.write("/scratch/u/stage/ns/shared/result.csv", "3\n")
	out, _ := newStage(t, "out", "/shared/result.csv")
	if err := out.Run(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(out.Local + "/shared/result.csv"); err != nil || string(got) != "3\n" {
		t.Errorf("staged out: %q %v", got, err)
	}
}

// Nothing is copied when one path cannot be: every path is checked first.
func TestStageRefusesBeforeCopying(t *testing.T) {
	f := newFakeRemote()
	in, _ := newStage(t, "in", "/shared/ok", "/shared/big")
	writeLocal(t, in.Local+"/shared/ok/a", "a", 0o644)
	writeLocal(t, in.Local+"/shared/big/b", strings.Repeat("x", 2048), 0o644)
	err := in.Run(context.Background(), f)
	if err == nil || !strings.Contains(err.Error(), "above FirecREST's direct transfer limit") {
		t.Fatalf("got %v", err)
	}
	if len(f.files) != 0 {
		t.Errorf("copied before refusing: %v", f.files)
	}

	in, _ = newStage(t, "in", "/shared/missing")
	if err := in.Run(context.Background(), f); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("missing input: %v", err)
	}
	out, _ := newStage(t, "out", "/shared/never-written")
	if err := out.Run(context.Background(), f); err == nil || !strings.Contains(err.Error(), "nothing at /scratch/u/stage/ns/shared/never-written") {
		t.Errorf("missing output: %v", err)
	}

	in, _ = newStage(t, "in", "/shared/ok")
	writeLocal(t, in.Local+"/shared/ok/a", "a", 0o644)
	in.OnVolume = func(string) error { return errors.New("not on a volume") }
	if err := in.Run(context.Background(), f); err == nil || err.Error() != "not on a volume" {
		t.Errorf("volume check: %v", err)
	}

	in.OnVolume = nil
	for _, p := range []string{"/ssh", "/ssh/client-secret", "/etc"} {
		in.Paths, in.Private = []string{p}, []string{"/ssh", "/etc/keytab"}
		if err := in.Run(context.Background(), f); err == nil || !strings.Contains(err.Error(), "credentials") {
			t.Errorf("%s: %v", p, err)
		}
	}
	in.Paths, in.Private = []string{"/shared/ok"}, nil
	in.User = "someone-else"
	if err := in.Run(context.Background(), f); err == nil || !strings.Contains(err.Error(), "runs as fireuser") {
		t.Errorf("user check: %v", err)
	}
	if len(f.files) != 0 {
		t.Errorf("copied after a refusal: %v", f.files)
	}
}
