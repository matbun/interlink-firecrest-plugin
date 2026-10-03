package bridge

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultSocket is where the daemon listens and the shims connect unless
// FIRECREST_BRIDGE_SOCKET says otherwise.
const DefaultSocket = "/var/run/firecrest-bridge/bridge.sock"

// Duration is a time.Duration written as "5s" in YAML.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	v, err := time.ParseDuration(n.Value)
	if err != nil {
		return fmt.Errorf("duration %q: %w", n.Value, err)
	}
	*d = Duration(v)
	return nil
}

// Config is the daemon configuration (FirecrestConfig.yaml).
type Config struct {
	// FirecrestURL is the FirecREST v2 base URL and System the cluster name in it.
	FirecrestURL string `yaml:"FirecrestURL"`
	System       string `yaml:"System"`

	// OAuth2 client credentials. The secret may come from a file (a mounted
	// Kubernetes Secret) or from FIRECREST_CLIENT_SECRET.
	TokenURL         string `yaml:"TokenURL"`
	ClientID         string `yaml:"ClientID"`
	ClientSecret     string `yaml:"ClientSecret"`
	ClientSecretFile string `yaml:"ClientSecretFile"`

	// APIKey selects the CSCS service-account proxy instead of client credentials.
	APIKey     string `yaml:"APIKey"`
	APIKeyFile string `yaml:"APIKeyFile"`

	// Account is passed to every job submission when set.
	Account string `yaml:"Account"`

	// JobRoot is the slurm plugin's DataRootFolder. It must be the same absolute
	// path in the plugin container and on the cluster: the plugin writes absolute
	// paths into job.slurm, and the bridge mirrors the tree under it one to one.
	JobRoot string `yaml:"JobRoot"`

	Socket string `yaml:"Socket"`

	// SyncInterval is how often the files a running job writes (logs, status
	// files) are pulled back from the cluster.
	SyncInterval Duration `yaml:"SyncInterval"`
	// FinalSyncRounds is how many more pulls follow the one done when a job is
	// first seen terminal, to pick up files flushed late.
	FinalSyncRounds int `yaml:"FinalSyncRounds"`
	// PingTTL caches the liveness check behind `squeue --me`.
	PingTTL Duration `yaml:"PingTTL"`
	// UploadParallelism bounds concurrent uploads when a job is submitted.
	UploadParallelism int `yaml:"UploadParallelism"`
	// MaxUploadSize is FirecREST's limit for direct uploads (max_ops_file_size).
	MaxUploadSize int64 `yaml:"MaxUploadSize"`
	// RequestTimeout bounds one FirecREST call.
	RequestTimeout Duration `yaml:"RequestTimeout"`
}

// LoadConfig reads path and applies environment overrides and defaults.
func LoadConfig(path string) (Config, error) {
	var c Config
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return c, err
		}
		if err := yaml.Unmarshal(b, &c); err != nil {
			return c, fmt.Errorf("%s: %w", path, err)
		}
	}
	c.applyEnv()
	if err := c.resolveSecrets(); err != nil {
		return c, err
	}
	c.applyDefaults()
	return c, c.Validate()
}

func (c *Config) applyEnv() {
	for env, field := range map[string]*string{
		"FIRECREST_URL":           &c.FirecrestURL,
		"FIRECREST_SYSTEM":        &c.System,
		"FIRECREST_TOKEN_URL":     &c.TokenURL,
		"FIRECREST_CLIENT_ID":     &c.ClientID,
		"FIRECREST_CLIENT_SECRET": &c.ClientSecret,
		"FIRECREST_API_KEY":       &c.APIKey,
		"FIRECREST_ACCOUNT":       &c.Account,
		"FIRECREST_JOB_ROOT":      &c.JobRoot,
		"FIRECREST_BRIDGE_SOCKET": &c.Socket,
	} {
		if v := os.Getenv(env); v != "" {
			*field = v
		}
	}
}

func (c *Config) resolveSecrets() error {
	read := func(file string, dst *string) error {
		if *dst != "" || file == "" {
			return nil
		}
		b, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		*dst = strings.TrimSpace(string(b))
		return nil
	}
	if err := read(c.ClientSecretFile, &c.ClientSecret); err != nil {
		return err
	}
	return read(c.APIKeyFile, &c.APIKey)
}

func (c *Config) applyDefaults() {
	if c.Socket == "" {
		c.Socket = DefaultSocket
	}
	if c.SyncInterval == 0 {
		c.SyncInterval = Duration(5 * time.Second)
	}
	if c.FinalSyncRounds == 0 {
		c.FinalSyncRounds = 2
	}
	if c.PingTTL == 0 {
		c.PingTTL = Duration(30 * time.Second)
	}
	if c.UploadParallelism == 0 {
		c.UploadParallelism = 4
	}
	if c.MaxUploadSize == 0 {
		c.MaxUploadSize = 5 * 1024 * 1024
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = Duration(60 * time.Second)
	}
	if c.JobRoot != "" {
		c.JobRoot = filepath.Clean(c.JobRoot)
	}
}

// Validate reports the first missing or inconsistent setting.
func (c Config) Validate() error {
	switch {
	case c.FirecrestURL == "":
		return errors.New("FirecrestURL is required")
	case c.System == "":
		return errors.New("System is required")
	case c.APIKey == "" && (c.TokenURL == "" || c.ClientID == "" || c.ClientSecret == ""):
		return errors.New("set APIKey, or TokenURL, ClientID and ClientSecret")
	case c.JobRoot == "" || !filepath.IsAbs(c.JobRoot) || c.JobRoot == "/":
		return errors.New("JobRoot must be an absolute path below /")
	}
	return nil
}
