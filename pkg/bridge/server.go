package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// request is what a shim sends to the daemon.
type request struct {
	Cmd  string   `json:"cmd"`
	Args []string `json:"args"`
}

// Handler serves the shims: POST /exec runs one command, GET /healthz answers ok.
func (b *Bridge) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /exec", func(w http.ResponseWriter, r *http.Request) {
		var req request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		res := b.Exec(r.Context(), req.Cmd, req.Args)
		if res.Code != 0 {
			b.log.Warn("command failed", "cmd", req.Cmd, "args", req.Args, "stderr", res.Stderr)
		} else {
			b.log.Debug("command", "cmd", req.Cmd, "args", req.Args)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(res)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok\n")
	})
	return mux
}

// Listen opens the unix socket the shims connect to, replacing a stale one.
func Listen(socket string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(socket), 0o755); err != nil {
		return nil, err
	}
	if err := os.Remove(socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	l, err := net.Listen("unix", socket)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(socket, 0o660); err != nil {
		l.Close()
		return nil, err
	}
	return l, nil
}

// RunShim forwards one Slurm command to the daemon and prints its result. It is
// what runs when the bridge binary is invoked as sbatch, squeue, scancel or sinfo.
func RunShim(cmd string, args []string, socket string, stdout, stderr io.Writer) int {
	timeout := 2 * time.Minute
	if cmd == "sbatch" {
		timeout = 15 * time.Minute // uploads the whole job directory
	}
	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socket)
			},
		},
	}
	if args == nil {
		args = []string{}
	}
	body, _ := json.Marshal(request{Cmd: cmd, Args: args})
	resp, err := client.Post("http://bridge/exec", "application/json", bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(stderr, "%s: error: firecrest-bridge daemon unreachable at %s: %v\n", cmd, socket, err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(stderr, "%s: error: firecrest-bridge daemon: %s %s\n", cmd, resp.Status, bytes.TrimSpace(msg))
		return 1
	}
	var res Result
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		fmt.Fprintf(stderr, "%s: error: firecrest-bridge daemon: bad answer: %v\n", cmd, err)
		return 1
	}
	io.WriteString(stdout, res.Stdout)
	io.WriteString(stderr, res.Stderr)
	return res.Code
}
