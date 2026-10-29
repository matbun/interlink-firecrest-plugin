package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/matbun/interlink-firecrest-plugin/pkg/firecrest"
)

// The emulated commands print what Slurm 24.11 prints for the same arguments,
// so the plugin parses them exactly as it parses the real ones.

// compactStates maps Slurm's long job state names to squeue's StateCompact codes.
var compactStates = map[string]string{
	"BOOT_FAIL":     "BF",
	"CANCELLED":     "CA",
	"COMPLETED":     "CD",
	"COMPLETING":    "CG",
	"CONFIGURING":   "CF",
	"DEADLINE":      "DL",
	"FAILED":        "F",
	"LAUNCH_FAILED": "LF",
	"NODE_FAIL":     "NF",
	"OUT_OF_MEMORY": "OOM",
	"PENDING":       "PD",
	"POWER_UP_NODE": "PU",
	"PREEMPTED":     "PR",
	"REQUEUED":      "RQ",
	"REQUEUE_FED":   "RF",
	"REQUEUE_HOLD":  "RH",
	"RESIZING":      "RS",
	"RESV_DEL_HOLD": "RD",
	"REVOKED":       "RV",
	"RUNNING":       "R",
	"SIGNALING":     "SI",
	"SPECIAL_EXIT":  "SE",
	"STAGE_OUT":     "SO",
	"STOPPED":       "ST",
	"SUSPENDED":     "S",
	"TIMEOUT":       "TO",
}

// activeStates are the codes of jobs that may still write to their directory.
var activeStates = map[string]bool{
	"PD": true, "R": true, "S": true, "CG": true, "CF": true, "RQ": true, "RF": true,
	"RH": true, "RS": true, "SI": true, "SO": true, "RD": true, "PU": true,
}

// compactState turns FirecREST's state ("CANCELLED by 1001", "RUNNING") into a
// StateCompact code.
func compactState(state string) string {
	word := strings.ToUpper(strings.TrimSpace(state))
	if i := strings.IndexAny(word, " ,+"); i >= 0 {
		word = word[:i]
	}
	if code, ok := compactStates[word]; ok {
		return code
	}
	return word
}

func isTerminal(code string) bool { return !activeStates[code] }

// sbatch uploads the job's directories and submits the script. The plugin
// calls it as `sbatch <dir>/job.slurm`.
func (b *Bridge) sbatch(ctx context.Context, args []string) Result {
	script := ""
	for _, a := range args {
		if strings.HasPrefix(a, "-") {
			return fail("sbatch: error: option %q is not supported by firecrest-bridge", a)
		}
		if script != "" {
			return fail("sbatch: error: only one batch script is supported")
		}
		script = a
	}
	if script == "" {
		return fail("sbatch: error: no batch script given")
	}
	script = filepath.Clean(script)
	dir := filepath.Dir(script)
	if !b.underRoot(dir) {
		return fail("sbatch: error: %s is not below the bridge JobRoot %s", script, b.cfg.JobRoot)
	}
	dirs, err := b.jobDirs(script)
	if err != nil {
		return fail("sbatch: error: Unable to open file %s", script)
	}

	seen := make(map[string]map[string]fileMeta, len(dirs))
	for _, d := range dirs {
		if err := b.uploadDir(ctx, d); err != nil {
			return fail("sbatch: error: staging %s through FirecREST: %v", d, err)
		}
		if seen[d], err = b.listFiles(ctx, d); err != nil {
			return fail("sbatch: error: listing %s through FirecREST: %v", d, err)
		}
	}
	id, err := b.remote.SubmitJob(ctx, firecrest.JobSpec{
		ScriptPath:       script,
		WorkingDirectory: dir,
		Account:          b.cfg.Account,
	})
	if err != nil {
		return fail("sbatch: error: %v", err)
	}

	b.mu.Lock()
	for _, d := range dirs {
		b.jobs[d] = &job{dir: d, id: id, seen: seen[d], state: "PD"}
	}
	b.mu.Unlock()
	b.log.Info("submitted", "job", id, "dir", dir, "dirs", len(dirs))
	return ok(fmt.Sprintf("Submitted batch job %s\n", id))
}

// jobDirs returns the script's directory, then every sibling pod directory
// that the script names by absolute path. A gang's script, in the head pod's
// directory, runs each member's job.sh and writes its logs in the member's own
// directory. The plugin writes job.sh into every member's directory before it
// submits, and puts them all next to the head's: directly below JobRoot, or
// below the base a job-workdir annotation names. Neither that base nor hidden
// directories (the plugin's .gang-heads) are job directories.
func (b *Bridge) jobDirs(script string) ([]string, error) {
	text, err := os.ReadFile(script)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(script)
	parent := filepath.Dir(dir)
	prefix := parent + "/"
	found := map[string]bool{}
	for s := string(text); ; {
		i := strings.Index(s, prefix)
		if i < 0 {
			break
		}
		// "/x/jobs/..." in "/y/x/jobs/..." is not a sibling's path.
		token := i == 0 || (!isPathByte(s[i-1]) && s[i-1] != '/')
		s = s[i+len(prefix):]
		end := 0
		for end < len(s) && isPathByte(s[end]) {
			end++
		}
		name := s[:end]
		if !token || name == "" || strings.HasPrefix(name, ".") {
			continue
		}
		d := filepath.Join(parent, name)
		if info, err := os.Lstat(d); d == dir || err != nil || !info.IsDir() {
			continue
		}
		if info, err := os.Lstat(filepath.Join(d, "job.sh")); err == nil && info.Mode().IsRegular() {
			found[d] = true
		}
	}
	dirs := []string{dir}
	for d := range found {
		dirs = append(dirs, d)
	}
	sort.Strings(dirs[1:])
	return dirs, nil
}

// isPathByte reports whether c can be part of a job directory name: the
// plugin names them <namespace>-<pod uid>.
func isPathByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.'
}

// squeue answers the two calls the plugin makes: `squeue --me` as a liveness
// gate, and `squeue --noheader -a --states=all -O exit_code,StateCompact -j <id>`.
func (b *Bridge) squeue(ctx context.Context, args []string) Result {
	var ids, fields []string
	noheader := false
	for i := 0; i < len(args); i++ {
		a := args[i]
		val := func() string {
			if k, v, found := strings.Cut(a, "="); found && strings.HasPrefix(k, "--") {
				return v
			}
			if i+1 < len(args) {
				i++
				return args[i]
			}
			return ""
		}
		switch {
		case a == "--me" || a == "-a" || a == "--all" || strings.HasPrefix(a, "--states") || a == "-t":
			if a == "-t" {
				i++
			}
		case a == "--noheader" || a == "-h":
			noheader = true
		case a == "-j" || strings.HasPrefix(a, "--jobs"):
			ids = append(ids, splitList(val())...)
		case a == "-O" || strings.HasPrefix(a, "--Format"):
			fields = append(fields, splitList(val())...)
		default:
			return fail("squeue: error: option %q is not supported by firecrest-bridge", a)
		}
	}

	if len(ids) == 0 {
		if err := b.ping(ctx); err != nil {
			return fail("squeue: error: FirecREST unreachable: %v", err)
		}
		return ok(squeueHeader)
	}
	if len(fields) == 0 {
		return fail("squeue: error: firecrest-bridge needs -O with -j")
	}
	for _, f := range fields {
		if !strings.EqualFold(f, "exit_code") && !strings.EqualFold(f, "StateCompact") {
			return fail("squeue: error: field %q is not supported by firecrest-bridge", f)
		}
	}

	var out strings.Builder
	if !noheader {
		for _, f := range fields {
			fmt.Fprintf(&out, "%-20s", strings.ToUpper(f))
		}
		out.WriteString("\n")
	}
	for _, id := range ids {
		j, err := b.getJob(ctx, id)
		if firecrest.IsNotFound(err) {
			return fail("slurm_load_jobs error: Invalid job id specified")
		}
		if err != nil {
			return fail("squeue: error: %v", err)
		}
		code := b.setState(id, j)
		if isTerminal(code) {
			b.finalSync(ctx, id)
		}
		for _, f := range fields {
			v := code
			if strings.EqualFold(f, "exit_code") {
				v = fmt.Sprintf("%d:%d", j.Status.ExitCode, j.Status.InterruptSignal)
			}
			fmt.Fprintf(&out, "%-20s", v)
		}
		out.WriteString("\n")
	}
	return ok(out.String())
}

// getJob asks FirecREST for a job. Every member of a gang polls the job they
// share, so for a job with several directories one answer serves all squeue
// calls within a SyncInterval.
func (b *Bridge) getJob(ctx context.Context, id string) (*firecrest.Job, error) {
	b.mu.Lock()
	dirs := 0
	for _, j := range b.jobs {
		if j.id == id {
			dirs++
		}
	}
	q := b.queries[id]
	if q == nil && dirs > 1 {
		q = &jobQuery{}
		b.queries[id] = q
	}
	b.mu.Unlock()
	if dirs < 2 {
		return b.remote.GetJob(ctx, id)
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.job != nil && time.Since(q.at) < time.Duration(b.cfg.SyncInterval) {
		return q.job, nil
	}
	j, err := b.remote.GetJob(ctx, id)
	if err != nil {
		return nil, err
	}
	q.job, q.at = j, time.Now()
	return j, nil
}

// setState records a job's state in the entry of each of its directories.
func (b *Bridge) setState(id string, j *firecrest.Job) string {
	code := compactState(j.Status.State)
	for _, tracked := range b.lookupAll(id) {
		tracked.mu.Lock()
		tracked.state = code
		tracked.mu.Unlock()
	}
	return code
}

// refresh asks for a job's state outside an squeue call.
func (b *Bridge) refresh(ctx context.Context, id string) {
	j, err := b.getJob(ctx, id)
	if err != nil {
		b.log.Warn("state refresh failed", "job", id, "err", err)
		return
	}
	b.setState(id, j)
}

const squeueHeader = "             JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)\n"

// ping checks the whole chain down to the cluster, at most once per PingTTL.
func (b *Bridge) ping(ctx context.Context) error {
	b.pingMu.Lock()
	defer b.pingMu.Unlock()
	if time.Since(b.pingAt) < time.Duration(b.cfg.PingTTL) {
		return nil
	}
	if _, err := b.remote.UserInfo(ctx); err != nil {
		return err
	}
	b.pingAt = time.Now()
	return nil
}

// scancel cancels jobs. Like Slurm it succeeds silently for unknown ids.
func (b *Bridge) scancel(ctx context.Context, args []string) Result {
	if len(args) == 0 {
		return fail("scancel: error: No job identification provided")
	}
	for _, a := range args {
		if strings.HasPrefix(a, "-") {
			return fail("scancel: error: option %q is not supported by firecrest-bridge", a)
		}
		if err := b.remote.CancelJob(ctx, a); err != nil && !firecrest.IsNotFound(err) && !b.finished(ctx, a) {
			return fail("scancel: error: %v", err)
		}
		b.log.Info("cancelled", "job", a)
	}
	return ok("")
}

// finished reports whether a job is gone or already in a terminal state. Slurm's
// scancel exits 0 for those, but prints a warning that FirecREST turns into an
// error.
func (b *Bridge) finished(ctx context.Context, id string) bool {
	j, err := b.remote.GetJob(ctx, id)
	if firecrest.IsNotFound(err) {
		return true
	}
	return err == nil && isTerminal(compactState(j.Status.State))
}

// sinfo answers `sinfo --json`, `sinfo --noheader -N --format=...` and `sinfo -s`.
func (b *Bridge) sinfo(ctx context.Context, args []string) Result {
	format, noheader, perNode, summary, asJSON := "", false, false, false, false
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--json":
			asJSON = true
		case a == "-s" || a == "--summarize":
			summary = true
		case a == "-N" || a == "--Node":
			perNode = true
		case a == "-h" || a == "--noheader":
			noheader = true
		case strings.HasPrefix(a, "--format="):
			format = strings.TrimPrefix(a, "--format=")
		case a == "-o" || a == "--format":
			if i+1 < len(args) {
				i++
				format = args[i]
			}
		default:
			return fail("sinfo: error: option %q is not supported by firecrest-bridge", a)
		}
	}

	nodes, err := b.remote.Nodes(ctx)
	if err != nil {
		return fail("sinfo: error: %v", err)
	}
	switch {
	case asJSON:
		return sinfoJSON(nodes)
	case summary:
		parts, err := b.remote.Partitions(ctx)
		if err != nil {
			return fail("sinfo: error: %v", err)
		}
		return ok(sinfoSummary(parts, nodes))
	case perNode && format != "":
		return ok(sinfoFormat(nodes, format, noheader))
	}
	return fail("sinfo: error: firecrest-bridge supports --json, -s and -N --format only")
}

// sinfoJSON prints the per-node shape the plugin's JSON parser reads.
// FirecREST does not report RealMemory; allocated plus free stands in for it.
func sinfoJSON(nodes []firecrest.Node) Result {
	type node struct {
		Name        string `json:"name"`
		CPUs        int64  `json:"cpus"`
		AllocCPUs   int64  `json:"alloc_cpus"`
		RealMemory  int64  `json:"real_memory"`
		FreeMemory  int64  `json:"free_memory"`
		AllocMemory int64  `json:"alloc_memory"`
	}
	out := struct {
		Nodes []node `json:"nodes"`
	}{Nodes: []node{}}
	for _, n := range nodes {
		out.Nodes = append(out.Nodes, node{
			Name: n.Name, CPUs: n.CPUs, AllocCPUs: n.AllocCPUs,
			RealMemory: n.AllocMemory + n.FreeMemory, FreeMemory: n.FreeMemory, AllocMemory: n.AllocMemory,
		})
	}
	b, err := json.Marshal(out)
	if err != nil {
		return fail("sinfo: error: %v", err)
	}
	return ok(string(b) + "\n")
}

// sinfoFormat expands %N %c %m %e %P %t %T for each node and partition, the way
// `sinfo -N` lists a node once per partition. Width modifiers are ignored.
func sinfoFormat(nodes []firecrest.Node, format string, noheader bool) string {
	headers := map[byte]string{'N': "NODELIST", 'c': "CPUS", 'm': "MEMORY", 'e': "FREE_MEM", 'P': "PARTITION", 't': "STATE", 'T': "STATE"}
	expand := func(n *firecrest.Node, part string) string {
		var sb strings.Builder
		for i := 0; i < len(format); i++ {
			if format[i] != '%' || i+1 >= len(format) {
				sb.WriteByte(format[i])
				continue
			}
			j := i + 1
			for j < len(format) && (format[j] == '.' || format[j] == '-' || (format[j] >= '0' && format[j] <= '9')) {
				j++
			}
			if j >= len(format) {
				sb.WriteString(format[i:])
				break
			}
			spec := format[j]
			i = j
			if n == nil {
				if h, ok := headers[spec]; ok {
					sb.WriteString(h)
				}
				continue
			}
			switch spec {
			case 'N':
				sb.WriteString(n.Name)
			case 'c':
				fmt.Fprintf(&sb, "%d", n.CPUs)
			case 'm':
				fmt.Fprintf(&sb, "%d", n.AllocMemory+n.FreeMemory)
			case 'e':
				fmt.Fprintf(&sb, "%d", n.FreeMemory)
			case 'P':
				sb.WriteString(part)
			case 't', 'T':
				sb.WriteString(strings.ToLower(strings.Join(n.State, "+")))
			}
		}
		return sb.String()
	}

	var out strings.Builder
	if !noheader {
		out.WriteString(expand(nil, "") + "\n")
	}
	for i := range nodes {
		parts := nodes[i].Partitions
		if len(parts) == 0 {
			parts = []string{""}
		}
		for _, p := range parts {
			out.WriteString(expand(&nodes[i], p) + "\n")
		}
	}
	return out.String()
}

// sinfoSummary mimics `sinfo -s`. FirecREST does not report time limits.
func sinfoSummary(parts []firecrest.Partition, nodes []firecrest.Node) string {
	var out strings.Builder
	fmt.Fprintf(&out, "%-9s %5s %10s %16s %s\n", "PARTITION", "AVAIL", "TIMELIMIT", "NODES(A/I/O/T)", "NODELIST")
	for _, p := range parts {
		var alloc, idle, other int
		var names []string
		for _, n := range nodes {
			if !contains(n.Partitions, p.Name) {
				continue
			}
			names = append(names, n.Name)
			switch state := strings.ToUpper(strings.Join(n.State, "+")); {
			case strings.Contains(state, "ALLOC") || strings.Contains(state, "MIX"):
				alloc++
			case strings.Contains(state, "IDLE"):
				idle++
			default:
				other++
			}
		}
		sort.Strings(names)
		fmt.Fprintf(&out, "%-9s %5s %10s %16s %s\n", p.Name, strings.ToLower(p.State), "n/a",
			fmt.Sprintf("%d/%d/%d/%d", alloc, idle, other, alloc+idle+other), strings.Join(names, ","))
	}
	return out.String()
}

func splitList(s string) []string {
	var out []string
	for _, f := range strings.Split(s, ",") {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
