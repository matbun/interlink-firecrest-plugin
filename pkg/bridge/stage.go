package bridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/matbun/interlink-firecrest-plugin/pkg/firecrest"
)

// StageRemote is the part of FirecREST a stage run uses; *firecrest.Client
// implements it.
type StageRemote interface {
	UserInfo(ctx context.Context) (string, error)
	Stat(ctx context.Context, path string) (*firecrest.Stat, error)
	Ls(ctx context.Context, path string, recursive, dereference bool) ([]firecrest.Entry, error)
	Mkdir(ctx context.Context, path string, parent bool) error
	Upload(ctx context.Context, dir, name string, data []byte) error
	Download(ctx context.Context, path string) ([]byte, error)
	Chmod(ctx context.Context, path, mode string) error
}

// Stage copies the paths of a workload's interlink.eu/stage-in annotation to
// the site before its job runs, and those of interlink.eu/stage-out back after
// it, as kueue-offload's stage Job (its executor's Staging.Command). A path p of
// the cluster side is Root+p on the site, where the job's container binds it
// back to p. Files are copied whole through FirecREST's direct transfers, so
// none may be larger than MaxFileSize.
type Stage struct {
	Direction string // in or out
	Paths     []string
	// Root is the namespace's stage directory on the site, STAGE_ROOT/STAGE_NAMESPACE.
	Root string
	// User is the site user of REMOTE; FirecREST must run the client as that user.
	User        string
	MaxFileSize int64
	Parallelism int
	// Local prefixes every path of the cluster side; empty in a stage Job.
	Local string
	// OnVolume refuses a path of the cluster side that is not on a mounted
	// volume: only the volumes the stage Job mounts are copied.
	OnVolume func(local string) error
	// Private are the stage Job's own mounts, its credentials: never copied.
	Private []string
	Out     io.Writer
}

// StageFromEnv reads the environment of kueue-offload's stage Job:
// STAGE_DIRECTION, STAGE_PATHS (':'-separated), STAGE_ROOT, STAGE_NAMESPACE,
// REMOTE and SSH_KEY. REMOTE is <user>@<FirecREST URL>?system=<system>&token=<token
// URL> (the executor's Staging.User and Staging.TransferHost); SSH_KEY is the
// mounted client secret, with the client id in the file client-id next to it.
// FIRECREST_URL, FIRECREST_SYSTEM, FIRECREST_TOKEN_URL, FIRECREST_CLIENT_ID(_FILE)
// and FIRECREST_CLIENT_SECRET(_FILE) win when set.
func StageFromEnv(getenv func(string) string) (Stage, firecrest.Options, error) {
	var s Stage
	var opts firecrest.Options
	s.Direction = getenv("STAGE_DIRECTION")
	if s.Direction != "in" && s.Direction != "out" {
		return s, opts, fmt.Errorf("STAGE_DIRECTION must be in or out, not %q", s.Direction)
	}
	for _, p := range strings.Split(getenv("STAGE_PATHS"), ":") {
		if p = strings.TrimSpace(p); p == "" {
			continue
		}
		clean, err := cleanStagePath(p)
		if err != nil {
			return s, opts, err
		}
		s.Paths = append(s.Paths, clean)
	}
	if len(s.Paths) == 0 {
		return s, opts, errors.New("STAGE_PATHS names no path")
	}
	root, ns := getenv("STAGE_ROOT"), getenv("STAGE_NAMESPACE")
	if !path.IsAbs(root) || path.Clean(root) != root || root == "/" || strings.ContainsAny(root, "'\n") {
		return s, opts, fmt.Errorf("STAGE_ROOT %q is not a clean absolute path below /", root)
	}
	if ns == "" || strings.ContainsAny(ns, "/'\n") || ns == "." || ns == ".." {
		return s, opts, fmt.Errorf("STAGE_NAMESPACE %q is not a namespace", ns)
	}
	s.Root = root + "/" + ns

	user, endpoint, _ := strings.Cut(getenv("REMOTE"), "@")
	s.User = user
	if endpoint != "" {
		u, err := url.Parse(endpoint)
		if err != nil {
			return s, opts, fmt.Errorf("REMOTE: %w", err)
		}
		q := u.Query()
		opts.System, opts.TokenURL = q.Get("system"), q.Get("token")
		u.RawQuery = ""
		opts.URL = u.String()
	}
	if v := getenv("FIRECREST_URL"); v != "" {
		opts.URL = v
	}
	if v := getenv("FIRECREST_SYSTEM"); v != "" {
		opts.System = v
	}
	if v := getenv("FIRECREST_TOKEN_URL"); v != "" {
		opts.TokenURL = v
	}
	key := getenv("SSH_KEY")
	for _, c := range []struct {
		env, file string
		dst       *string
	}{
		{"FIRECREST_CLIENT_ID", filepath.Join(filepath.Dir(key), "client-id"), &opts.ClientID},
		{"FIRECREST_CLIENT_SECRET", key, &opts.ClientSecret},
	} {
		*c.dst = getenv(c.env)
		file := getenv(c.env + "_FILE")
		if file == "" && key != "" {
			file = c.file
		}
		if *c.dst != "" || file == "" {
			continue
		}
		b, err := os.ReadFile(file)
		if err != nil {
			return s, opts, err
		}
		*c.dst = strings.TrimSpace(string(b))
	}
	switch {
	case opts.URL == "" || opts.System == "":
		return s, opts, errors.New("no FirecREST URL and system: set REMOTE to <user>@<URL>?system=<system>&token=<token URL>")
	case opts.TokenURL == "" || opts.ClientID == "" || opts.ClientSecret == "":
		return s, opts, errors.New("no FirecREST client: set the token URL in REMOTE and SSH_KEY to the mounted client secret")
	}
	for _, f := range []string{key, getenv("KEYTAB")} {
		if f != "" {
			s.Private = append(s.Private, filepath.Dir(f))
		}
	}
	s.MaxFileSize = 5 * 1024 * 1024
	s.Parallelism = 4
	s.OnVolume = onMountedVolume
	s.Out = os.Stdout
	return s, opts, nil
}

// cleanStagePath drops one trailing slash and refuses a relative path, an empty,
// . or .. segment, and quotes and newlines, which FirecREST's commands cannot
// carry.
func cleanStagePath(p string) (string, error) {
	if !path.IsAbs(p) {
		return "", fmt.Errorf("refused: %s is not an absolute path", p)
	}
	clean := strings.TrimSuffix(p, "/")
	if clean == "" || path.Clean(clean) != clean {
		return "", fmt.Errorf("refused: %s has an empty, . or .. segment", p)
	}
	if strings.ContainsAny(clean, "'\n") {
		return "", fmt.Errorf("refused: %s contains a quote or a newline", p)
	}
	return clean, nil
}

// onMountedVolume refuses a path whose first directory is not a mount point of
// its own: the stage Job mounts only the volumes the paths live on.
func onMountedVolume(local string) error {
	top := "/" + strings.SplitN(strings.TrimPrefix(local, "/"), "/", 2)[0]
	var a, b syscall.Stat_t
	if err := syscall.Stat(top, &a); err != nil {
		return fmt.Errorf("refused: %s is not on a mounted volume: %w", local, err)
	}
	if err := syscall.Stat("/", &b); err != nil {
		return err
	}
	if a.Dev == b.Dev {
		return fmt.Errorf("refused: %s is not on a mounted volume (%s is not a mount point)", local, top)
	}
	return nil
}

// stageFile is one regular file to copy, by its paths on both sides.
type stageFile struct {
	local, remote string
	size          int64
	exec          bool
}

// stageLink is one symbolic link of the site to recreate on the cluster side.
type stageLink struct {
	local, remote, target string
}

// stagePlan is everything a run copies, checked whole before the first copy.
type stagePlan struct {
	dirs  []string // to create, on the receiving side
	files []stageFile
	links []stageLink
}

// Run copies every path, after checking all of them.
func (s Stage) Run(ctx context.Context, remote StageRemote) error {
	if s.User != "" {
		who, err := remote.UserInfo(ctx)
		if err != nil {
			return fmt.Errorf("FirecREST: %w", err)
		}
		if who != s.User {
			return fmt.Errorf("the FirecREST client runs as %s, but REMOTE names the site user %s", who, s.User)
		}
	}
	var plan stagePlan
	for _, p := range s.Paths {
		local := s.Local + p
		for _, d := range s.Private {
			if p == d || strings.HasPrefix(p, d+"/") || strings.HasPrefix(d, p+"/") {
				return fmt.Errorf("refused: %s holds the stage Job's credentials (%s)", p, d)
			}
		}
		if s.OnVolume != nil {
			if err := s.OnVolume(local); err != nil {
				return err
			}
		}
		var err error
		if s.Direction == "in" {
			err = s.planIn(&plan, p)
		} else {
			err = s.planOut(ctx, remote, &plan, p)
		}
		if err != nil {
			return err
		}
	}
	for _, f := range plan.files {
		if f.size > s.MaxFileSize {
			return fmt.Errorf("stage %s: %s is %d bytes, above FirecREST's direct transfer limit of %d bytes", s.Direction, f.local, f.size, s.MaxFileSize)
		}
	}
	var err error
	if s.Direction == "in" {
		err = s.copyIn(ctx, remote, plan)
	} else {
		err = s.copyOut(ctx, remote, plan)
	}
	if err != nil {
		return err
	}
	var bytes int64
	for _, f := range plan.files {
		bytes += f.size
	}
	links := ""
	if len(plan.links) > 0 {
		links = fmt.Sprintf(", %d links", len(plan.links))
	}
	fmt.Fprintf(s.Out, "stage %s: %d files, %d bytes%s: done\n", s.Direction, len(plan.files), bytes, links)
	return nil
}

// planIn adds a path of the cluster side, a file or a directory tree. Links
// are followed, as rsync --copy-links: FirecREST makes only links to paths of
// the site, which the job's container may not see.
func (s Stage) planIn(plan *stagePlan, p string) error {
	local, remote := s.Local+p, s.Root+p
	info, err := os.Stat(local)
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stage in: %s does not exist", p)
	}
	if err != nil {
		return err
	}
	fmt.Fprintf(s.Out, "stage in: %s\n", p)
	if !info.IsDir() {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("stage in: %s is not a file or a directory", p)
		}
		plan.dirs = append(plan.dirs, path.Dir(remote))
		plan.files = append(plan.files, stageFile{local: local, remote: remote, size: info.Size(), exec: info.Mode()&0o111 != 0})
		return nil
	}
	return s.walkIn(plan, local, remote, nil)
}

// walkIn adds a directory and everything below it. above holds the real paths
// of the directories it is in, so a link back to one of them is not followed.
func (s Stage) walkIn(plan *stagePlan, local, remote string, above []string) error {
	real, err := filepath.EvalSymlinks(local)
	if err != nil {
		return err
	}
	if slices.Contains(above, real) {
		fmt.Fprintf(s.Out, "stage in: skipping %s, a link to a directory it is in\n", local)
		return nil
	}
	above = append(above, real)
	plan.dirs = append(plan.dirs, remote)
	entries, err := os.ReadDir(local)
	if err != nil {
		return err
	}
	for _, e := range entries {
		lp, rp := filepath.Join(local, e.Name()), remote+"/"+e.Name()
		if strings.ContainsAny(e.Name(), "'\n") {
			return fmt.Errorf("stage in: %s contains a quote or a newline, which FirecREST cannot copy", lp)
		}
		info, err := os.Stat(lp)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			fmt.Fprintf(s.Out, "stage in: skipping %s, a link to nothing\n", lp)
		case err != nil:
			return err
		case info.IsDir():
			if err := s.walkIn(plan, lp, rp, above); err != nil {
				return err
			}
		case info.Mode().IsRegular():
			plan.files = append(plan.files, stageFile{local: lp, remote: rp, size: info.Size(), exec: info.Mode()&0o111 != 0})
		default:
			fmt.Fprintf(s.Out, "stage in: skipping %s, not a file or a directory\n", lp)
		}
	}
	return nil
}

// planOut adds a path of the site, a file or a directory tree.
func (s Stage) planOut(ctx context.Context, remote StageRemote, plan *stagePlan, p string) error {
	local, rp := s.Local+p, s.Root+p
	st, err := remote.Stat(ctx, rp)
	if firecrest.IsNotFound(err) {
		return fmt.Errorf("stage out: %s: nothing at %s on the site", p, rp)
	}
	if err != nil {
		return fmt.Errorf("stage out: %s: %w", p, err)
	}
	fmt.Fprintf(s.Out, "stage out: %s\n", p)
	switch st.Mode & 0o170000 {
	case 0o040000:
	case 0o100000, 0o120000:
		plan.dirs = append(plan.dirs, filepath.Dir(local))
		plan.files = append(plan.files, stageFile{local: local, remote: rp, size: st.Size, exec: st.Mode&0o100 != 0})
		return nil
	default:
		return fmt.Errorf("stage out: %s is not a file or a directory on the site", p)
	}
	entries, err := remote.Ls(ctx, rp, true, false)
	if err != nil {
		return fmt.Errorf("stage out: listing %s: %w", rp, err)
	}
	plan.dirs = append(plan.dirs, local)
	for _, e := range entries {
		name := path.Clean(e.Name)
		if path.IsAbs(name) || name == "." || name == ".." || strings.HasPrefix(name, "../") || strings.ContainsAny(name, "'\n") {
			return fmt.Errorf("stage out: %s lists %q, which is not a name below it", rp, e.Name)
		}
		lp, from := filepath.Join(local, filepath.FromSlash(name)), rp+"/"+name
		switch e.Type {
		case "d":
			plan.dirs = append(plan.dirs, lp)
		case "-":
			plan.files = append(plan.files, stageFile{local: lp, remote: from, size: e.Size.Int64(), exec: len(e.Permissions) > 2 && e.Permissions[2] == 'x'})
		case "l":
			plan.links = append(plan.links, stageLink{local: lp, remote: from, target: e.LinkTarget})
		default:
			fmt.Fprintf(s.Out, "stage out: skipping %s, not a file, directory or link\n", from)
		}
	}
	return nil
}

// parallel runs fn on every index with at most n at a time and joins the errors.
func parallel(n, count int, fn func(i int) error) error {
	errs := make([]error, count)
	sem := make(chan struct{}, max(n, 1))
	var wg sync.WaitGroup
	for i := range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			errs[i] = fn(i)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (s Stage) copyIn(ctx context.Context, remote StageRemote, plan stagePlan) error {
	// mkdir -p of the leaves creates every directory.
	sort.Strings(plan.dirs)
	for i, d := range plan.dirs {
		if i+1 < len(plan.dirs) && (plan.dirs[i+1] == d || strings.HasPrefix(plan.dirs[i+1], d+"/")) {
			continue
		}
		if err := remote.Mkdir(ctx, d, true); err != nil {
			return fmt.Errorf("mkdir %s: %w", d, err)
		}
	}
	err := parallel(s.Parallelism, len(plan.files), func(i int) error {
		f := plan.files[i]
		data, err := os.ReadFile(f.local)
		if err != nil {
			return err
		}
		if err := remote.Upload(ctx, path.Dir(f.remote), path.Base(f.remote), data); err != nil {
			return fmt.Errorf("upload %s: %w", f.local, err)
		}
		if f.exec {
			if err := remote.Chmod(ctx, f.remote, "755"); err != nil {
				return fmt.Errorf("chmod %s: %w", f.remote, err)
			}
		}
		return nil
	})
	return err
}

func (s Stage) copyOut(ctx context.Context, remote StageRemote, plan stagePlan) error {
	for _, d := range plan.dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	err := parallel(s.Parallelism, len(plan.files), func(i int) error {
		f := plan.files[i]
		data, err := remote.Download(ctx, f.remote)
		if err != nil {
			return fmt.Errorf("download %s: %w", f.remote, err)
		}
		mode := fs.FileMode(0o644)
		if f.exec {
			mode = 0o755
		}
		// A link of the same name would be followed: it goes first.
		if info, err := os.Lstat(f.local); err == nil && info.Mode()&fs.ModeSymlink != 0 {
			if err := os.Remove(f.local); err != nil {
				return err
			}
		}
		if err := os.WriteFile(f.local, data, mode); err != nil {
			return err
		}
		return os.Chmod(f.local, mode)
	})
	if err != nil {
		return err
	}
	for _, l := range plan.links {
		if err := os.Remove(l.local); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err := os.Symlink(l.target, l.local); err != nil {
			return err
		}
	}
	return nil
}
