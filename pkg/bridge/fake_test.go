package bridge

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/matbun/interlink-firecrest-plugin/pkg/firecrest"
)

// fakeRemote is an in-memory FirecREST: a filesystem and a scheduler.
type fakeRemote struct {
	mu      sync.Mutex
	files   map[string]*fakeFile
	dirs    map[string]bool
	jobs    map[string]*firecrest.Job
	nextID  int
	clock   int
	specs   []firecrest.JobSpec
	views   []viewCall
	removed []string
	pings   int
	nodes   []firecrest.Node
	parts   []firecrest.Partition
}

type fakeFile struct {
	data  []byte
	mode  string
	mtime int
}

type viewCall struct {
	path         string
	offset, size int64
}

func newFakeRemote() *fakeRemote {
	return &fakeRemote{
		files: map[string]*fakeFile{},
		dirs:  map[string]bool{"/": true},
		jobs:  map[string]*firecrest.Job{},
		nodes: []firecrest.Node{
			{Name: "nid001", CPUs: 2, AllocCPUs: 1, FreeMemory: 15650, AllocMemory: 1000, State: []string{"MIXED"}, Partitions: []string{"normal", "debug"}},
		},
		parts: []firecrest.Partition{{Name: "normal", State: "UP"}, {Name: "debug", State: "UP"}},
	}
}

func notFound(path string) error {
	return &firecrest.APIError{Method: "GET", Path: path, Status: http.StatusNotFound, Body: `{"message":"not found"}`}
}

// write sets a remote file as a job would, bumping its mtime.
func (f *fakeRemote) write(path, content string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clock++
	f.files[path] = &fakeFile{data: []byte(content), mode: "644", mtime: f.clock}
	for d := filepath.Dir(path); ; d = filepath.Dir(d) {
		f.dirs[d] = true
		if d == "/" {
			break
		}
	}
}

func (f *fakeRemote) read(path string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, ok := f.files[path]
	if !ok {
		return "", false
	}
	return string(file.data), true
}

func (f *fakeRemote) setState(id, state string, exit, signal int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobs[id].Status = firecrest.JobStatus{State: state, ExitCode: exit, InterruptSignal: signal}
}

func (f *fakeRemote) SubmitJob(_ context.Context, spec firecrest.JobSpec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.files[spec.ScriptPath]; spec.ScriptPath != "" && !ok {
		return "", fmt.Errorf("sbatch: error: Unable to open file %s", spec.ScriptPath)
	}
	f.nextID++
	id := fmt.Sprint(f.nextID)
	f.specs = append(f.specs, spec)
	f.jobs[id] = &firecrest.Job{JobID: firecrest.Flexible(id), Status: firecrest.JobStatus{State: "PENDING"}}
	return id, nil
}

func (f *fakeRemote) GetJob(_ context.Context, id string) (*firecrest.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[id]
	if !ok {
		return nil, notFound("/compute/sys/jobs/" + id)
	}
	cp := *j
	return &cp, nil
}

func (f *fakeRemote) CancelJob(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[id]
	if !ok {
		return notFound("/compute/sys/jobs/" + id)
	}
	// What FirecREST 2.6 answers, because scancel prints this on stderr.
	if isTerminal(compactState(j.Status.State)) {
		return &firecrest.APIError{Method: "DELETE", Path: "/compute/sys/jobs/" + id, Status: http.StatusInternalServerError,
			Body: `{"message":"Unexpected Slurm command response. exit_status:0 std_err:scancel: error: Kill job error on job id ` + id + `: Job/step already completing or completed\n"}`}
	}
	j.Status = firecrest.JobStatus{State: "CANCELLED by 1001"}
	return nil
}

func (f *fakeRemote) Nodes(context.Context) ([]firecrest.Node, error) { return f.nodes, nil }

func (f *fakeRemote) Partitions(context.Context) ([]firecrest.Partition, error) {
	return f.parts, nil
}

func (f *fakeRemote) UserInfo(context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pings++
	return "fireuser", nil
}

func (f *fakeRemote) Ls(_ context.Context, path string, recursive bool) ([]firecrest.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.dirs[path] {
		return nil, notFound(path)
	}
	var out []firecrest.Entry
	under := func(p string) (string, bool) {
		rel, err := filepath.Rel(path, p)
		if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
			return "", false
		}
		if !recursive && strings.Contains(rel, "/") {
			return "", false
		}
		return rel, true
	}
	for d := range f.dirs {
		if rel, ok := under(d); ok {
			out = append(out, firecrest.Entry{Name: rel, Type: "d", Size: "4096"})
		}
	}
	for p, file := range f.files {
		if rel, ok := under(p); ok {
			out = append(out, firecrest.Entry{
				Name: rel, Type: "-", Size: firecrest.Flexible(fmt.Sprint(len(file.data))),
				LastModified: fmt.Sprintf("mtime-%d", file.mtime),
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (f *fakeRemote) Mkdir(_ context.Context, path string, parent bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !parent && !f.dirs[filepath.Dir(path)] {
		return notFound(path)
	}
	for d := path; ; d = filepath.Dir(d) {
		f.dirs[d] = true
		if d == "/" {
			break
		}
	}
	return nil
}

func (f *fakeRemote) Upload(_ context.Context, dir, name string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.dirs[dir] {
		return notFound(dir)
	}
	f.clock++
	f.files[filepath.Join(dir, name)] = &fakeFile{data: append([]byte(nil), data...), mode: "644", mtime: f.clock}
	return nil
}

func (f *fakeRemote) Chmod(_ context.Context, path, mode string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, ok := f.files[path]
	if !ok {
		return notFound(path)
	}
	file.mode = mode
	return nil
}

func (f *fakeRemote) View(_ context.Context, path string, offset, size int64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.views = append(f.views, viewCall{path, offset, size})
	file, ok := f.files[path]
	if !ok {
		return nil, notFound(path)
	}
	if offset >= int64(len(file.data)) {
		return []byte{}, nil
	}
	end := min(offset+size, int64(len(file.data)))
	return append([]byte(nil), file.data[offset:end]...), nil
}

func (f *fakeRemote) Rm(_ context.Context, path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.dirs[path] && f.files[path] == nil {
		return notFound(path)
	}
	f.removed = append(f.removed, path)
	for p := range f.files {
		if p == path || strings.HasPrefix(p, path+"/") {
			delete(f.files, p)
		}
	}
	for d := range f.dirs {
		if d == path || strings.HasPrefix(d, path+"/") {
			delete(f.dirs, d)
		}
	}
	return nil
}
