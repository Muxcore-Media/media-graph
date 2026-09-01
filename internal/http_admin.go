package internal

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
)

func (m *Module) registerAdminRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/graph", m.requireGraphAuth(m.handleGraphSummary))
	mux.HandleFunc("/api/graph/nodes", m.requireGraphAuth(m.handleGraphNodes))
	mux.HandleFunc("/api/graph/node", m.requireGraphAuth(m.handleGraphNode))
	mux.HandleFunc("/api/graph/related", m.requireGraphAuth(m.handleGraphRelated))
	mux.HandleFunc("/api/graph/search", m.requireGraphAuth(m.handleGraphSearch))
	mux.HandleFunc("/api/graph/neighbors", m.requireGraphAuth(m.handleGraphNeighbors))
	mux.HandleFunc("/api/graph/path", m.requireGraphAuth(m.handleGraphPath))
	mux.HandleFunc("/api/graph/edges", m.requireGraphAuth(m.handleGraphEdges))
	mux.HandleFunc("/api/graph/link", m.requireGraphAuth(m.handleGraphLink))
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
		"edges": m.store.EdgeCount(),
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
	switch r.Method {
	case http.MethodGet:
		m.getGraphNode(w, r)
	case http.MethodPost:
		m.postGraphNode(w, r)
	case http.MethodDelete:
		m.deleteGraphNode(w, r)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "GET/POST/DELETE only")
	}
}

func (m *Module) getGraphNode(w http.ResponseWriter, r *http.Request) {
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

func (m *Module) postGraphNode(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read body")
		return
	}
	var req struct {
		Node Node `json:"node"`
	}
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		writeErr(w, http.StatusBadRequest, "invalid json")
		return
	}
	m.cfgMu.RLock()
	auto := m.autoLink
	m.cfgMu.RUnlock()
	out, _, err := m.store.UpsertNodeWithAutoLink(req.Node, auto)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"node": out})
}

func (m *Module) deleteGraphNode(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		writeErr(w, http.StatusBadRequest, "id required")
		return
	}
	cascade := r.URL.Query().Get("cascade") == "1" || r.URL.Query().Get("cascade") == "true"
	if err := m.store.DeleteNode(id, cascade); err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeErr(w, http.StatusNotFound, err.Error())
			return
		}
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
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

func (m *Module) handleGraphNeighbors(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	id := r.URL.Query().Get("id")
	if id == "" {
		writeErr(w, http.StatusBadRequest, "id required")
		return
	}
	rel := r.URL.Query().Get("rel")
	depth, _ := strconv.Atoi(r.URL.Query().Get("depth"))
	n, edges, nodes, err := m.store.Neighbors(id, rel, depth)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeErr(w, http.StatusNotFound, err.Error())
			return
		}
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"node": n, "edges": edges, "nodes": nodes})
}

func (m *Module) handleGraphPath(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	fromID := r.URL.Query().Get("from_id")
	toID := r.URL.Query().Get("to_id")
	if fromID == "" || toID == "" {
		writeErr(w, http.StatusBadRequest, "from_id and to_id required")
		return
	}
	maxDepth, _ := strconv.Atoi(r.URL.Query().Get("max_depth"))
	ids, edges, found, err := m.store.Path(fromID, toID, maxDepth)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeErr(w, http.StatusNotFound, err.Error())
			return
		}
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"node_ids": ids, "edges": edges, "found": found})
}

func (m *Module) handleGraphEdges(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"edges": m.store.ListEdges()})
}

func (m *Module) handleGraphLink(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		m.postGraphLink(w, r)
	case http.MethodDelete:
		m.deleteGraphLink(w, r)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "POST/DELETE only")
	}
}

func (m *Module) postGraphLink(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read body")
		return
	}
	var req struct {
		FromID string  `json:"from_id"`
		ToID   string  `json:"to_id"`
		Rel    string  `json:"rel"`
		Weight float64 `json:"weight"`
	}
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		writeErr(w, http.StatusBadRequest, "invalid json")
		return
	}
	rel := req.Rel
	if rel == "" {
		m.cfgMu.RLock()
		rel = m.defaultRel
		m.cfgMu.RUnlock()
	}
	e, err := m.store.Link(req.FromID, req.ToID, rel, req.Weight)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"edge": e})
}

func (m *Module) deleteGraphLink(w http.ResponseWriter, r *http.Request) {
	edgeID := r.URL.Query().Get("edge_id")
	if edgeID == "" {
		writeErr(w, http.StatusBadRequest, "edge_id required")
		return
	}
	if err := m.store.Unlink(edgeID); err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeErr(w, http.StatusNotFound, err.Error())
			return
		}
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

var errNotFound = errString("not found")

type errString string

func (e errString) Error() string { return string(e) }
