package firecrest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"syscall"
	"time"

	"golang.org/x/oauth2"
)

// Retry is how a call that failed transiently is tried again: at most Attempts
// tries in all, Delay before the second and twice as long before each next one,
// and, when MaxElapsed is set, no new try once that has passed since the first
// started. Each retry is logged once to Log, or to slog's default logger.
type Retry struct {
	Attempts   int
	Delay      time.Duration
	MaxElapsed time.Duration
	Log        *slog.Logger
}

// DefaultRetry tries a call 4 times over about 30 s, waiting 2, 4 and 8 s. A
// call then lasts at most 30 s plus its last try, which keeps a job whose
// submission meets a few failures within the 120 s in which the interLink API
// server waits for the plugin's answer to sbatch.
var DefaultRetry = Retry{Attempts: 4, Delay: 2 * time.Second, MaxElapsed: 30 * time.Second}

// Tries is one call's way through a Retry.
type Tries struct {
	r       Retry
	start   time.Time
	attempt int
	delay   time.Duration
}

// Start begins a call; Again then decides after each failure.
func (r Retry) Start() *Tries {
	return &Tries{r: r, start: time.Now(), attempt: 1, delay: r.Delay}
}

// Again reports whether a call that failed with err is tried again: err is
// Transient, and attempts, time and ctx are left. Before it returns true it
// logs the failure and waits.
func (t *Tries) Again(ctx context.Context, what string, err error) bool {
	if !Transient(err) || ctx.Err() != nil || t.attempt >= t.r.Attempts {
		return false
	}
	wait := t.delay
	if jitter := wait / 4; jitter > 0 {
		wait += rand.N(jitter) // the parallel uploads of a job do not retry in step
	}
	if t.r.MaxElapsed > 0 && time.Since(t.start)+wait > t.r.MaxElapsed {
		return false
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < wait {
		return false
	}
	log := t.r.Log
	if log == nil {
		log = slog.Default()
	}
	log.Warn("FirecREST call failed, retrying", "call", what, "attempt", t.attempt, "of", t.r.Attempts, "in", wait.Round(time.Millisecond), "err", err)
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
	}
	t.attempt++
	t.delay *= 2
	return true
}

// Transient reports whether err may go away when the same call is sent again:
// FirecREST, a proxy in front of it or the token endpoint answered 502, 503 or
// 504, the connection was refused, reset or closed early, or the call timed
// out. A 4xx never is.
func Transient(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return transientStatus(apiErr.Status)
	}
	var tokenErr *oauth2.RetrieveError
	if errors.As(err, &tokenErr) {
		return tokenErr.Response != nil && transientStatus(tokenErr.Response.StatusCode)
	}
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func transientStatus(code int) bool {
	return code == http.StatusBadGateway || code == http.StatusServiceUnavailable || code == http.StatusGatewayTimeout
}

// NotSent reports whether a call that failed with err never reached FirecREST:
// no connection, or no token. A submission that failed so did not queue a job.
func NotSent(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return true
	}
	var tokenErr *oauth2.RetrieveError
	return errors.As(err, &tokenErr)
}
