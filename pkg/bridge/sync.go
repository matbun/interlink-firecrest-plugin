package bridge

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/matbun/interlink-firecrest-plugin/pkg/firecrest"
)

// smallFile is the size up to which a changed remote file is fetched whole.
// Above it, a file that grew is assumed to be a log and only its tail is
// fetched. Status and probe files are rewritten in place and stay far below it.
const smallFile = 64 * 1024

// uploadDir mirrors the local directory tree to the cluster: every directory,
// including empty ones (emptyDir volumes), and every regular file, keeping the
// executable bit.
func (b *Bridge) uploadDir(ctx context.Context, dir string) error {
	type file struct {
		path string
		mode fs.FileMode
		size int64
	}
	var dirs []string
	var files []file
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		switch {
		case d.IsDir():
			dirs = append(dirs, p)
		case info.Mode().IsRegular():
			if info.Size() > b.cfg.MaxUploadSize {
				return fmt.Errorf("%s is %d bytes, above the direct upload limit of %d", p, info.Size(), b.cfg.MaxUploadSize)
			}
			files = append(files, file{path: p, mode: info.Mode().Perm(), size: info.Size()})
		default:
			b.log.Warn("not uploading non-regular file", "path", p)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// mkdir -p of the leaves creates every directory.
	sort.Strings(dirs)
	for i, d := range dirs {
		if i+1 < len(dirs) && strings.HasPrefix(dirs[i+1], d+"/") {
			continue
		}
		if err := b.remote.Mkdir(ctx, d, true); err != nil {
			return err
		}
	}

	errs := make(chan error, len(files))
	sem := make(chan struct{}, b.cfg.UploadParallelism)
	var wg sync.WaitGroup
	for _, f := range files {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			data, err := os.ReadFile(f.path)
			if err != nil {
				errs <- err
				return
			}
			if err := b.remote.Upload(ctx, filepath.Dir(f.path), filepath.Base(f.path), data); err != nil {
				errs <- fmt.Errorf("upload %s: %w", f.path, err)
				return
			}
			if f.mode&0o111 != 0 {
				if err := b.remote.Chmod(ctx, f.path, fmt.Sprintf("%o", f.mode)); err != nil {
					errs <- fmt.Errorf("chmod %s: %w", f.path, err)
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	return errors.Join(drain(errs)...)
}

func drain(ch <-chan error) []error {
	var out []error
	for err := range ch {
		out = append(out, err)
	}
	return out
}

// pluginReads reports whether the slurm plugin reads a file of this name back
// from a job directory: job and container logs (*.out), container and probe
// status files (*.status, *.timestamp), and compute-node. Everything else the
// job leaves there, such as the images the enroot runtime imports, stays remote.
func pluginReads(name string) bool {
	if strings.Contains(name, "/") {
		return false
	}
	switch {
	case name == "compute-node",
		strings.HasSuffix(name, ".out"),
		strings.HasSuffix(name, ".status"),
		strings.HasSuffix(name, ".timestamp"):
		return true
	}
	return false
}

// listFiles returns the files in dir on the cluster that the plugin reads.
func (b *Bridge) listFiles(ctx context.Context, dir string) (map[string]fileMeta, error) {
	entries, err := b.remote.Ls(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	out := make(map[string]fileMeta, len(entries))
	for _, e := range entries {
		if e.Type != "-" || !pluginReads(e.Name) {
			continue
		}
		out[e.Name] = fileMeta{size: e.Size.Int64(), mtime: e.LastModified}
	}
	return out, nil
}

// Run pulls job output on every SyncInterval until ctx is done.
func (b *Bridge) Run(ctx context.Context) {
	t := time.NewTicker(time.Duration(b.cfg.SyncInterval))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.SyncAll(ctx)
		}
	}
}

// SyncAll does one round: pull every unfinished job, and drop the jobs whose
// local directory the plugin has removed.
func (b *Bridge) SyncAll(ctx context.Context) {
	// Without the job root (a volume not mounted yet, say) a missing job
	// directory says nothing about the pod, so nothing is removed remotely.
	_, rootErr := os.Stat(b.cfg.JobRoot)
	for _, j := range b.snapshot() {
		if _, err := os.Stat(j.dir); errors.Is(err, fs.ErrNotExist) && rootErr == nil {
			b.forget(ctx, j)
			continue
		}
		j.mu.Lock()
		// A pending job has written nothing yet, and may wait in the queue for hours.
		if !j.done && j.state != "PD" {
			if err := b.pull(ctx, j); err != nil {
				b.log.Warn("sync failed", "job", j.id, "dir", j.dir, "err", err)
			} else if j.terminal {
				j.rounds++
				j.done = j.rounds > b.cfg.FinalSyncRounds
			}
		}
		j.mu.Unlock()
	}
}

// finalSync pulls a job's directory before its terminal state is reported, so
// the plugin finds the container status files when it reads them.
func (b *Bridge) finalSync(ctx context.Context, id string) {
	j := b.lookup(id)
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.terminal {
		return
	}
	if err := b.pull(ctx, j); err != nil {
		b.log.Warn("final sync failed", "job", id, "dir", j.dir, "err", err)
		return
	}
	j.terminal = true
}

// pull copies the files the plugin reads that changed since the last pull into
// the local directory. Files are written in place, never replaced, so the
// plugin can keep following a log through an open descriptor. Callers hold j.mu.
func (b *Bridge) pull(ctx context.Context, j *job) error {
	files, err := b.listFiles(ctx, j.dir)
	if err != nil {
		return err
	}
	var errs []error
	for name, meta := range files {
		if old, ok := j.seen[name]; ok && old == meta {
			continue
		}
		path := filepath.Join(j.dir, name)
		if err := b.fetch(ctx, path, path, meta.size); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			continue
		}
		j.seen[name] = meta
	}
	return errors.Join(errs...)
}

// fetch brings local up to date with a remote file of the given size.
func (b *Bridge) fetch(ctx context.Context, remote, local string, size int64) error {
	if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(local, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}

	from := int64(0)
	if size > smallFile && info.Size() <= size {
		from = info.Size() // a growing log: fetch the tail only
	}
	for off := from; off < size; {
		n := min(size-off, int64(firecrest.MaxViewSize))
		data, err := b.remote.View(ctx, remote, off, n)
		if err != nil {
			return err
		}
		if len(data) == 0 {
			size = off // the file shrank since it was listed
			break
		}
		if _, err := f.WriteAt(data, off); err != nil {
			return err
		}
		off += int64(len(data))
	}
	return f.Truncate(size)
}

// forget drops a job whose local directory is gone and removes its remote copy.
func (b *Bridge) forget(ctx context.Context, j *job) {
	b.mu.Lock()
	delete(b.jobs, j.dir)
	b.mu.Unlock()
	if !b.underRoot(j.dir) {
		return
	}
	if err := b.remote.Rm(ctx, j.dir); err != nil && !firecrest.IsNotFound(err) {
		b.log.Warn("remote cleanup failed", "dir", j.dir, "err", err)
		return
	}
	b.log.Info("removed", "job", j.id, "dir", j.dir)
}

// Recover registers the jobs the plugin already knows about, from the
// JobID.jid files it keeps in each job directory, so that a restarted bridge
// keeps syncing them.
func (b *Bridge) Recover() error {
	entries, err := os.ReadDir(b.cfg.JobRoot)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(b.cfg.JobRoot, e.Name())
		raw, err := os.ReadFile(filepath.Join(dir, "JobID.jid"))
		if err != nil {
			continue
		}
		id := strings.TrimSpace(string(raw))
		if id == "" {
			continue
		}
		b.mu.Lock()
		if _, ok := b.jobs[dir]; !ok {
			b.jobs[dir] = &job{dir: dir, id: id, seen: map[string]fileMeta{}}
			b.log.Info("recovered", "job", id, "dir", dir)
		}
		b.mu.Unlock()
	}
	return nil
}
