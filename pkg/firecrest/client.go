// Package firecrest is a minimal client for the FirecREST v2 API, limited to the
// calls the bridge needs: job submission and status, node status, and the
// synchronous filesystem operations.
package firecrest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// MaxViewSize is the largest chunk requested from ops/view in one call. It
// matches FirecREST's default max_ops_file_size (5 MiB).
const MaxViewSize = 5 * 1024 * 1024

// Options configures a Client. Either APIKey or the client-credentials triple
// (TokenURL, ClientID, ClientSecret) must be set.
type Options struct {
	URL    string // FirecREST base URL, e.g. https://api.cscs.ch/hpc/firecrest/v2
	System string // target system name, e.g. daint

	TokenURL     string
	ClientID     string
	ClientSecret string

	// APIKey selects the CSCS service-account proxy, which takes the key in the
	// X-API-Key header instead of an OAuth2 bearer token.
	APIKey string

	Timeout time.Duration
}

// Client talks to one system of one FirecREST deployment.
type Client struct {
	base   string
	system string
	http   *http.Client
}

// APIError is a non-2xx answer from FirecREST.
type APIError struct {
	Method string
	Path   string
	Status int
	Body   string
}

func (e *APIError) Error() string {
	msg := e.Body
	var parsed struct {
		Message string `json:"message"`
	}
	if json.Unmarshal([]byte(e.Body), &parsed) == nil && parsed.Message != "" {
		msg = parsed.Message
	}
	return fmt.Sprintf("firecrest %s %s: %d %s", e.Method, e.Path, e.Status, msg)
}

// IsNotFound reports whether err is a FirecREST 404.
func IsNotFound(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound
}

// New builds a Client. With client credentials the token is fetched lazily and
// refreshed shortly before it expires.
func New(ctx context.Context, opts Options) (*Client, error) {
	if opts.URL == "" || opts.System == "" {
		return nil, errors.New("firecrest: URL and System are required")
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	base := &http.Client{Timeout: timeout}

	var hc *http.Client
	switch {
	case opts.APIKey != "":
		hc = &http.Client{
			Timeout:   timeout,
			Transport: apiKeyTransport{key: opts.APIKey, next: http.DefaultTransport},
		}
	case opts.TokenURL != "" && opts.ClientID != "" && opts.ClientSecret != "":
		cc := clientcredentials.Config{
			ClientID:     opts.ClientID,
			ClientSecret: opts.ClientSecret,
			TokenURL:     opts.TokenURL,
			AuthStyle:    oauth2.AuthStyleInParams,
		}
		hc = cc.Client(context.WithValue(ctx, oauth2.HTTPClient, base))
		hc.Timeout = timeout
	default:
		return nil, errors.New("firecrest: set either APIKey or TokenURL, ClientID and ClientSecret")
	}

	return &Client{
		base:   strings.TrimRight(opts.URL, "/"),
		system: opts.System,
		http:   hc,
	}, nil
}

type apiKeyTransport struct {
	key  string
	next http.RoundTripper
}

func (t apiKeyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("X-API-Key", t.key)
	return t.next.RoundTrip(req)
}

// System returns the system name the client targets.
func (c *Client) System() string { return c.system }

func (c *Client) do(ctx context.Context, method, path string, query url.Values, body io.Reader, contentType string, out any) error {
	u := c.base + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("firecrest %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("firecrest %s %s: reading body: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &APIError{Method: method, Path: path, Status: resp.StatusCode, Body: strings.TrimSpace(string(data))}
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("firecrest %s %s: decoding %q: %w", method, path, truncate(data, 200), err)
	}
	return nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, query url.Values, in, out any) error {
	var body io.Reader
	contentType := ""
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
		contentType = "application/json"
	}
	return c.do(ctx, method, path, query, body, contentType, out)
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

func (c *Client) computePath(rest string) string {
	return "/compute/" + url.PathEscape(c.system) + rest
}

func (c *Client) opsPath(op string) string {
	return "/filesystem/" + url.PathEscape(c.system) + "/ops/" + op
}

// Flexible decodes a JSON string or number into a string. FirecREST returns
// sizes as strings and some ids as numbers.
type Flexible string

func (f *Flexible) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*f = ""
		return nil
	}
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*f = Flexible(s)
		return nil
	}
	*f = Flexible(strings.TrimSpace(string(b)))
	return nil
}

// Int64 parses the value, returning 0 when it is empty or not a number.
func (f Flexible) Int64() int64 {
	n, _ := strconv.ParseInt(string(f), 10, 64)
	return n
}

// JobSpec is the body of a job submission. WorkingDirectory is required by
// FirecREST; exactly one of Script and ScriptPath should be set.
type JobSpec struct {
	Name             string            `json:"name,omitempty"`
	Account          string            `json:"account,omitempty"`
	Script           string            `json:"script,omitempty"`
	ScriptPath       string            `json:"scriptPath,omitempty"`
	WorkingDirectory string            `json:"workingDirectory"`
	Env              map[string]string `json:"env,omitempty"`
}

// SubmitJob submits a batch job and returns its scheduler id.
func (c *Client) SubmitJob(ctx context.Context, spec JobSpec) (string, error) {
	var out struct {
		JobID Flexible `json:"jobId"`
	}
	if err := c.doJSON(ctx, http.MethodPost, c.computePath("/jobs"), nil, map[string]JobSpec{"job": spec}, &out); err != nil {
		return "", err
	}
	if out.JobID == "" {
		return "", errors.New("firecrest: job submitted but no jobId returned")
	}
	return string(out.JobID), nil
}

// JobStatus is the scheduler state of a job. State is Slurm's long name,
// sometimes followed by detail ("CANCELLED by 1001").
type JobStatus struct {
	State           string `json:"state"`
	StateReason     string `json:"stateReason"`
	ExitCode        int    `json:"exitCode"`
	InterruptSignal int    `json:"interruptSignal"`
}

// Job is the subset of FirecREST's JobModel the bridge uses.
type Job struct {
	JobID            Flexible  `json:"jobId"`
	Name             string    `json:"name"`
	Status           JobStatus `json:"status"`
	Nodes            string    `json:"nodes"`
	WorkingDirectory string    `json:"workingDirectory"`
}

// GetJob returns one job. A job the scheduler does not know is a not-found error.
func (c *Client) GetJob(ctx context.Context, id string) (*Job, error) {
	var out struct {
		Jobs []Job `json:"jobs"`
	}
	path := c.computePath("/jobs/" + url.PathEscape(id))
	if err := c.doJSON(ctx, http.MethodGet, path, nil, nil, &out); err != nil {
		return nil, err
	}
	if len(out.Jobs) == 0 {
		return nil, &APIError{Method: http.MethodGet, Path: path, Status: http.StatusNotFound, Body: "no job in response"}
	}
	return &out.Jobs[0], nil
}

// CancelJob cancels a job.
func (c *Client) CancelJob(ctx context.Context, id string) error {
	return c.doJSON(ctx, http.MethodDelete, c.computePath("/jobs/"+url.PathEscape(id)), nil, nil, nil)
}

// Node is the subset of FirecREST's node model the bridge uses. Memory is in MB.
type Node struct {
	Name        string   `json:"name"`
	CPUs        int64    `json:"cpus"`
	AllocCPUs   int64    `json:"allocCpus"`
	IdleCPUs    int64    `json:"idleCpus"`
	FreeMemory  int64    `json:"freeMemory"`
	AllocMemory int64    `json:"allocMemory"`
	State       []string `json:"state"`
	Partitions  []string `json:"partitions"`
}

// Nodes returns the compute nodes of the system.
func (c *Client) Nodes(ctx context.Context) ([]Node, error) {
	var out struct {
		Nodes []Node `json:"nodes"`
	}
	err := c.doJSON(ctx, http.MethodGet, "/status/"+url.PathEscape(c.system)+"/nodes", nil, nil, &out)
	return out.Nodes, err
}

// Partition is a Slurm partition. State holds "UP" or "DOWN".
type Partition struct {
	Name       string `json:"name"`
	CPUs       int64  `json:"cpus"`
	TotalNodes int64  `json:"totalNodes"`
	State      string `json:"partition"`
}

// Partitions returns the partitions of the system.
func (c *Client) Partitions(ctx context.Context) ([]Partition, error) {
	var out struct {
		Partitions []Partition `json:"partitions"`
	}
	err := c.doJSON(ctx, http.MethodGet, "/status/"+url.PathEscape(c.system)+"/partitions", nil, nil, &out)
	return out.Partitions, err
}

// UserInfo returns the account FirecREST runs commands as. It doubles as a
// cheap liveness check of the whole chain down to the cluster.
func (c *Client) UserInfo(ctx context.Context) (string, error) {
	var out struct {
		User struct {
			Name string `json:"name"`
		} `json:"user"`
	}
	err := c.doJSON(ctx, http.MethodGet, "/status/"+url.PathEscape(c.system)+"/userinfo", nil, nil, &out)
	return out.User.Name, err
}

// Entry is one ls result. Name is relative to the listed path, with "/" between
// components when the listing is recursive.
type Entry struct {
	Name         string   `json:"name"`
	Type         string   `json:"type"` // "-" file, "d" directory, "l" link
	Size         Flexible `json:"size"`
	LastModified string   `json:"lastModified"`
	Permissions  string   `json:"permissions"`
}

// Ls lists path, including hidden files.
func (c *Client) Ls(ctx context.Context, path string, recursive bool) ([]Entry, error) {
	q := url.Values{"path": {path}, "showHidden": {"true"}}
	if recursive {
		q.Set("recursive", "true")
	}
	var out struct {
		Output []Entry `json:"output"`
	}
	err := c.doJSON(ctx, http.MethodGet, c.opsPath("ls"), q, nil, &out)
	return out.Output, err
}

// Mkdir creates path, and its parents when parent is set (mkdir -p).
func (c *Client) Mkdir(ctx context.Context, path string, parent bool) error {
	return c.doJSON(ctx, http.MethodPost, c.opsPath("mkdir"), nil, map[string]any{"path": path, "parent": parent}, nil)
}

// Upload writes data to dir/name. FirecREST rejects files above its
// max_ops_file_size (5 MiB by default) on this endpoint.
func (c *Client) Upload(ctx context.Context, dir, name string, data []byte) error {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", name)
	if err != nil {
		return err
	}
	if _, err := fw.Write(data); err != nil {
		return err
	}
	if err := mw.Close(); err != nil {
		return err
	}
	return c.do(ctx, http.MethodPost, c.opsPath("upload"), url.Values{"path": {dir}}, &buf, mw.FormDataContentType(), nil)
}

// Chmod sets the mode of path; mode is octal, e.g. "755".
func (c *Client) Chmod(ctx context.Context, path, mode string) error {
	return c.doJSON(ctx, http.MethodPut, c.opsPath("chmod"), nil, map[string]string{"path": path, "mode": mode}, nil)
}

// View returns up to size bytes of path starting at offset. Reading past the
// end returns no data and no error.
func (c *Client) View(ctx context.Context, path string, offset, size int64) ([]byte, error) {
	q := url.Values{
		"path":   {path},
		"offset": {strconv.FormatInt(offset, 10)},
		"size":   {strconv.FormatInt(size, 10)},
	}
	var out struct {
		Output string `json:"output"`
	}
	if err := c.doJSON(ctx, http.MethodGet, c.opsPath("view"), q, nil, &out); err != nil {
		return nil, err
	}
	return []byte(out.Output), nil
}

// Stat is the subset of the stat output the bridge uses.
type Stat struct {
	Mode  int64 `json:"mode"`
	Size  int64 `json:"size"`
	MTime int64 `json:"mtime"`
}

// Stat returns the metadata of path; a missing path is a not-found error.
func (c *Client) Stat(ctx context.Context, path string) (*Stat, error) {
	var out struct {
		Output Stat `json:"output"`
	}
	if err := c.doJSON(ctx, http.MethodGet, c.opsPath("stat"), url.Values{"path": {path}}, nil, &out); err != nil {
		return nil, err
	}
	return &out.Output, nil
}

// Rm removes path recursively. A missing path is a not-found error.
func (c *Client) Rm(ctx context.Context, path string) error {
	return c.doJSON(ctx, http.MethodDelete, c.opsPath("rm"), url.Values{"path": {path}}, nil, nil)
}
