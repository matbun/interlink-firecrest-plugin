package bridge

import (
	"context"
	"encoding/json"
	"fmt"
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

// sbatch uploads the directory of the batch script and submits it. The plugin
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

	if err := b.uploadDir(ctx, dir); err != nil {
		return fail("sbatch: error: staging %s through FirecREST: %v", dir, err)
	}
	seen, err := b.listFiles(ctx, dir)
	if err != nil {
		return fail("sbatch: error: listing %s through FirecREST: %v", dir, err)
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
	b.jobs[dir] = &job{dir: dir, id: id, seen: seen}
	b.mu.Unlock()
	b.log.Info("submitted", "job", id, "dir", dir)
	return ok(fmt.Sprintf("Submitted batch job %s\n", id))
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
		j, err := b.remote.GetJob(ctx, id)
		if firecrest.IsNotFound(err) {
			return fail("slurm_load_jobs error: Invalid job id specified")
		}
		if err != nil {
			return fail("squeue: error: %v", err)
		}
		code := compactState(j.Status.State)
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
