package server

import (
	"context"
	"net/http"
	"time"

	"github.com/ani03sha/kv-fabric/consistency"
	"github.com/ani03sha/kv-fabric/store"
	"go.uber.org/zap"
)

// This is a private type for context keys in this package. Using a named type (not plain string) prevents key
// collisions with other packages that might use context.WithValue.
type contextKey string

const (
	consistencyModeKey contextKey = "consistency_mode"
)

// This is a middleware that reads the X-Consistency header and attaches the parsed ConsistencyMode to the request context.
//
// Every downstream handler retrieves it via ConsistencyModeFromCtx — they never parse the header themselves.
// This is the single point where the HTTP header convention maps to the internal ConsistencyMode type.
func WithConsistencyMode(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mode := consistency.ParseConsistencyMode(r.Header.Get("X-Consistency"))
		ctx := context.WithValue(r.Context(), consistencyModeKey, mode)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Retrieves the ConsistencyMode set by WithConsistencyMode. Returns ConsistencyStrong if the middleware was not
// in the chain: safe default.
func ConsistencyModeFromCtx(ctx context.Context) store.ConsistencyMode {
	if mode, ok := ctx.Value(consistencyModeKey).(store.ConsistencyMode); ok {
		return mode
	}
	return store.ConsistencyStrong
}

// This function wraps http.ResponseWriter to capture the response status code.
// http.ResponseWriter does not expose the status code after it's written,
// so we intercept WriteHeader to record it for the metrics middleware.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.status = http.StatusOK
		r.wroteHeader = true
	}
	return r.ResponseWriter.Write(b)
}

func WithMetrics(metrics *Metrics, logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(rec, r)

			durationMs := float64(time.Since(start).Microseconds())
			mode := ConsistencyModeFromCtx(r.Context()).String()
			opType := methodToOpType(r.Method)

			metrics.RecordOpDuration(opType, mode, durationMs)

			logger.Debug("request served",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Int("status", rec.status),
				zap.Float64("duration_ms", durationMs),
				zap.String("consistency", mode),
			)
		})
	}
}

// Maps HTTP methods to operation type strings for metric labels.
func methodToOpType(method string) string {
	switch method {
	case http.MethodGet:
		return "get"
	case http.MethodPut:
		return "put"
	case http.MethodDelete:
		return "delete"
	default:
		return "other"
	}
}
