package bridge

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigFromFiles(t *testing.T) {
	dir := t.TempDir()
	write := func(name, value string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(value+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	t.Setenv("FIRECREST_URL", "http://firecrest.test")
	t.Setenv("FIRECREST_SYSTEM", "cluster")
	t.Setenv("FIRECREST_TOKEN_URL", "http://token.test")
	t.Setenv("FIRECREST_JOB_ROOT", "/scratch/user/jobs/ns1")
	t.Setenv("FIRECREST_CLIENT_ID_FILE", write("client-id", "my-client"))
	t.Setenv("FIRECREST_CLIENT_SECRET_FILE", write("client-secret", "my-secret"))
	t.Setenv("FIRECREST_ACCOUNT_FILE", write("account", "my-account"))

	c, err := LoadConfig("")
	if err != nil {
		t.Fatal(err)
	}
	if c.ClientID != "my-client" || c.ClientSecret != "my-secret" || c.Account != "my-account" {
		t.Fatalf("got id %q secret %q account %q", c.ClientID, c.ClientSecret, c.Account)
	}

	// A value set directly wins over its file.
	t.Setenv("FIRECREST_ACCOUNT", "other-account")
	if c, err = LoadConfig(""); err != nil {
		t.Fatal(err)
	}
	if c.Account != "other-account" {
		t.Fatalf("account %q, want the variable's", c.Account)
	}

	t.Setenv("FIRECREST_CLIENT_ID_FILE", filepath.Join(dir, "missing"))
	if _, err := LoadConfig(""); err == nil {
		t.Fatal("a missing credential file must fail")
	}
}

func TestBelowJobRoot(t *testing.T) {
	c := Config{JobRoot: "/scratch/user/jobs/ns1"}
	for path, want := range map[string]string{
		"/scratch/user/jobs/ns1":        "/scratch/user/jobs/ns1",
		"/scratch/user/jobs/ns1/":       "/scratch/user/jobs/ns1",
		"/scratch/user/jobs/ns1/pod-a":  "/scratch/user/jobs/ns1/pod-a",
		"/scratch/user/jobs/ns1/x/../y": "/scratch/user/jobs/ns1/y",
		"/scratch/user/jobs/ns10":       "",
		"/scratch/user/jobs":            "",
		"/scratch/user/jobs/ns1/../ns2": "",
		"scratch/user/jobs/ns1/pod-a":   "",
		"/":                             "",
	} {
		got, err := c.BelowJobRoot(path)
		if (err == nil) != (want != "") || got != want {
			t.Errorf("BelowJobRoot(%q) = %q, %v; want %q", path, got, err, want)
		}
	}
}
