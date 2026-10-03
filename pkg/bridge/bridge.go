// Package bridge lets the unmodified interLink slurm plugin drive a cluster
// through FirecREST. The plugin runs sbatch, squeue, scancel and sinfo shims
// that forward to this daemon, and reads and writes its job directories on a
// local filesystem that the daemon mirrors to and from the cluster.
package bridge

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/matbun/interlink-firecrest-plugin/pkg/firecrest"
)

// Remote is the part of FirecREST the bridge uses; *firecrest.Client implements it.
type Remote interface {
	SubmitJob(ctx context.Context, spec firecrest.JobSpec) (string, error)
	GetJob(ctx context.Context, id string) (*firecrest.Job, error)
	CancelJob(ctx context.Context, id string) error
	Nodes(ctx context.Context) ([]firecrest.Node, error)
	Partitions(ctx context.Context) ([]firecrest.Partition, error)
	UserInfo(ctx context.Context) (string, error)
	Ls(ctx context.Context, path string, recursive bool) ([]firecrest.Entry, error)
	Mkdir(ctx context.Context, path string, parent bool) error
	Upload(ctx context.Context, dir, name string, data []byte) error
	Chmod(ctx context.Context, path, mode string) error
	View(ctx context.Context, path string, offset, size int64) ([]byte, error)
	Rm(ctx context.Context, path string) error
}

// Result is what a shim prints and exits with.
type Result struct {
	Stdout string `json:"stdout"`
	Stderr string `json:"stderr"`
	Code   int    `json:"code"`
}

func ok(stdout string) Result { return Result{Stdout: stdout} }

func fail(format string, args ...any) Result {
	msg := format
	if len(args) > 0 {
		msg = fmt.Sprintf(format, args...)
	}
	if !strings.HasSuffix(msg, "\n") {
		msg += "\n"
	}
	return Result{Stderr: msg, Code: 1}
}

// Bridge is the daemon state: the jobs it submitted and the directories it mirrors.
type Bridge struct {
	cfg    Config
	remote Remote
	log    *slog.Logger

	mu   sync.Mutex
	jobs map[string]*job // key: job directory, the same path locally and remotely

	pingMu sync.Mutex
	pingAt time.Time
}

// job is one submitted job and the sync state of its directory.
type job struct {
	mu       sync.Mutex // serialises syncs of this directory
	dir      string
	id       string
	seen     map[string]fileMeta // remote file -> metadata at the last pull
	terminal bool
	rounds   int  // pulls since the job was first seen terminal
	done     bool // no more pulls needed
}

type fileMeta struct {
	size  int64
	mtime string
}

// New returns a Bridge. Call Recover before serving to pick up jobs submitted
// by a previous run.
func New(cfg Config, remote Remote, log *slog.Logger) *Bridge {
	if log == nil {
		log = slog.Default()
	}
	return &Bridge{cfg: cfg, remote: remote, log: log, jobs: map[string]*job{}}
}

// Exec runs one emulated Slurm command.
func (b *Bridge) Exec(ctx context.Context, cmd string, args []string) Result {
	switch cmd {
	case "sbatch":
		return b.sbatch(ctx, args)
	case "squeue":
		return b.squeue(ctx, args)
	case "scancel":
		return b.scancel(ctx, args)
	case "sinfo":
		return b.sinfo(ctx, args)
	}
	return fail("firecrest-bridge: unknown command %q", cmd)
}

// underRoot reports whether path is strictly below the job root, so that it is
// safe to mirror, and above all to remove remotely.
func (b *Bridge) underRoot(path string) bool {
	if !filepath.IsAbs(path) {
		return false
	}
	rel, err := filepath.Rel(b.cfg.JobRoot, filepath.Clean(path))
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, "../")
}

func (b *Bridge) lookup(id string) *job {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, j := range b.jobs {
		if j.id == id {
			return j
		}
	}
	return nil
}

func (b *Bridge) snapshot() []*job {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*job, 0, len(b.jobs))
	for _, j := range b.jobs {
		out = append(out, j)
	}
	return out
}
