package internal

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

func (m *Module) registerAdminRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/graph", m.handleGraphSummary)
	mux.HandleFunc("/api/graph/nodes", m.handleGraphNodes)
	mux.HandleFunc("/api/graph/node", m.handleGraphNode)
	mux.HandleFunc("/api/graph/related", m.handleGraphRelated)
	mux.HandleFunc("/api/graph/search", m.handleGraphSearch)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (m *Module) handleGraphSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	nodes := m.store.ListNodes()
	writeJSON(w, http.StatusOK, map[string]any{
		"nodes": len(nodes),
		"ok":    true,
	})
}

func (m *Module) handleGraphNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	kind := r.URL.Query().Get("kind")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items := m.store.Search("", kind, limit)
	writeJSON(w, http.StatusOK, map[string]any{"nodes": items})
}

func (m *Module) handleGraphNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	id := r.URL.Query().Get("id")
	ext := r.URL.Query().Get("external_id")
	var n *Node
	var err error
	if id != "" {
		n, err = m.store.GetNode(id)
	} else if ext != "" {
		n = m.store.FindByExternalID(ext)
		if n == nil {
			err = errNotFound
		}
	} else {
		writeErr(w, http.StatusBadRequest, "id or external_id required")
		return
	}
	if err != nil || n == nil {
		writeErr(w, http.StatusNotFound, "node not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"node": n})
}

func (m *Module) handleGraphRelated(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	id := r.URL.Query().Get("id")
	ext := r.URL.Query().Get("external_id")
	rel := r.URL.Query().Get("rel")
	depth, _ := strconv.Atoi(r.URL.Query().Get("depth"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	n, related, err := m.store.RelatedTitles(id, ext, rel, depth, limit)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeErr(w, http.StatusNotFound, err.Error())
			return
		}
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"node": n, "related": related})
}

func (m *Module) handleGraphSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	q := r.URL.Query().Get("q")
	kind := r.URL.Query().Get("kind")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items := m.store.Search(q, kind, limit)
	writeJSON(w, http.StatusOK, map[string]any{"nodes": items})
}

var errNotFound = errString("not found")

type errString string

func (e errString) Error() string { return string(e) }
