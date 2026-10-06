// firecrest-bridge lets the interLink slurm plugin reach a cluster through
// FirecREST. Run it as `firecrest-bridge daemon` next to the plugin, and point
// the plugin's SbatchPath, SqueuePath, ScancelPath and SinfoPath at the shims
// that `firecrest-bridge install-shims <dir>` creates.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/matbun/interlink-firecrest-plugin/pkg/bridge"
	"github.com/matbun/interlink-firecrest-plugin/pkg/firecrest"
)

var version = "dev"

var shimNames = []string{"sbatch", "squeue", "scancel", "sinfo"}

func main() {
	name := filepath.Base(os.Args[0])
	for _, s := range shimNames {
		if name == s {
			os.Exit(bridge.RunShim(name, os.Args[1:], socket(), os.Stdout, os.Stderr))
		}
	}

	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}
	switch os.Args[1] {
	case "daemon":
		if err := daemon(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "firecrest-bridge:", err)
			os.Exit(1)
		}
	case "shim":
		if len(os.Args) < 3 {
			usage(os.Stderr)
			os.Exit(2)
		}
		os.Exit(bridge.RunShim(os.Args[2], os.Args[3:], socket(), os.Stdout, os.Stderr))
	case "install-shims":
		if len(os.Args) != 3 {
			usage(os.Stderr)
			os.Exit(2)
		}
		if err := installShims(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, "firecrest-bridge:", err)
			os.Exit(1)
		}
	case "version":
		fmt.Println(version)
	default:
		usage(os.Stderr)
		os.Exit(2)
	}
}

func usage(w io.Writer) {
	fmt.Fprint(w, `usage:
  firecrest-bridge daemon [--config FirecrestConfig.yaml] [--verbose]
  firecrest-bridge install-shims <dir>
  firecrest-bridge shim <sbatch|squeue|scancel|sinfo> [args...]
  firecrest-bridge version
`)
}

func socket() string {
	if s := os.Getenv("FIRECREST_BRIDGE_SOCKET"); s != "" {
		return s
	}
	return bridge.DefaultSocket
}

func daemon(args []string) error {
	fs := flag.NewFlagSet("daemon", flag.ContinueOnError)
	configPath := fs.String("config", os.Getenv("FIRECRESTCONFIGPATH"), "path to FirecrestConfig.yaml")
	verbose := fs.Bool("verbose", false, "log every forwarded command")
	if err := fs.Parse(args); err != nil {
		return err
	}
	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	cfg, err := bridge.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client, err := firecrest.New(ctx, firecrest.Options{
		URL:          cfg.FirecrestURL,
		System:       cfg.System,
		TokenURL:     cfg.TokenURL,
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		APIKey:       cfg.APIKey,
		Timeout:      time.Duration(cfg.RequestTimeout),
	})
	if err != nil {
		return err
	}
	if user, err := client.UserInfo(ctx); err != nil {
		log.Warn("FirecREST not reachable yet", "url", cfg.FirecrestURL, "system", cfg.System, "err", err)
	} else {
		log.Info("FirecREST reachable", "url", cfg.FirecrestURL, "system", cfg.System, "user", user)
	}

	b := bridge.New(cfg, client, log)
	if err := b.Recover(); err != nil {
		return fmt.Errorf("recovering jobs from %s: %w", cfg.JobRoot, err)
	}
	l, err := bridge.Listen(cfg.Socket)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: b.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go b.Run(ctx)
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			log.Warn("shutdown", "err", err)
		}
	}()
	log.Info("listening", "socket", cfg.Socket, "jobRoot", cfg.JobRoot, "version", version)
	if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// installShims copies this binary into dir and links the Slurm command names to
// it, so a volume shared with the plugin container carries everything it needs.
func installShims(dir string) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := os.ReadFile(self)
	if err != nil {
		return err
	}
	target := filepath.Join(dir, "firecrest-bridge")
	tmp := target + ".tmp"
	if err := os.WriteFile(tmp, data, 0o755); err != nil {
		return err
	}
	if err := os.Rename(tmp, target); err != nil {
		return err
	}
	for _, s := range shimNames {
		link := filepath.Join(dir, s)
		if err := os.Remove(link); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.Symlink("firecrest-bridge", link); err != nil {
			return err
		}
	}
	fmt.Printf("installed %s and %v in %s\n", target, shimNames, dir)
	return nil
}
