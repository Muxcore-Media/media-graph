package internal

import (
	"net"
	"net/http"
	"os"
	"strings"
)

func moduleTokenFromEnv() string {
	for _, k := range []string{"GRAPH_MODULE_TOKEN", "MUXCORE_MODULE_TOKEN", "MUXCORE_TOKEN"} {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

func bearerToken(header string) string {
	const prefix = "Bearer "
	header = strings.TrimSpace(header)
	if len(header) > len(prefix) && strings.EqualFold(header[:len(prefix)], prefix) {
		return strings.TrimSpace(header[len(prefix):])
	}
	return header
}

func (m *Module) requireGraphAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if m.adminToken != "" {
			got := bearerToken(r.Header.Get("Authorization"))
			if got == "" {
				got = strings.TrimSpace(r.Header.Get("X-Admin-Token"))
			}
			if got != m.adminToken {
				writeErr(w, http.StatusUnauthorized, "admin token required")
				return
			}
		} else if !requestFromLoopback(r) {
			writeErr(w, http.StatusUnauthorized, "graph API requires admin token or loopback access")
			return
		}
		next(w, r)
	}
}

func requestFromLoopback(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	host = strings.TrimPrefix(host, "[")
	host = strings.TrimSuffix(host, "]")
	return host == "127.0.0.1" || host == "::1" || host == "localhost"
}
