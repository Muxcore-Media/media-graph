package internal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
)

type Node struct {
	Attrs      map[string]string
	ID         string
	Kind       string
	Title      string
	ExternalID string
}

type Edge struct {
	ID     string
	FromID string
	ToID   string
	Rel    string
	Weight float64
}

type Store struct {
	db    *sql.DB
	nodes map[string]*Node
	edges map[string]*Edge
	out   map[string][]string
	in    map[string][]string
	mu    sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		nodes: map[string]*Node{},
		edges: map[string]*Edge{},
		out:   map[string][]string{},
		in:    map[string][]string{},
	}
}

func (s *Store) UpsertNode(n Node) (*Node, error) {
	if strings.TrimSpace(n.Kind) == "" || strings.TrimSpace(n.Title) == "" {
		return nil, fmt.Errorf("kind and title required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if n.ID == "" {
		n.ID = "gn_" + uuid.NewString()[:8]
	}
	if n.Attrs == nil {
		n.Attrs = map[string]string{}
	}
	cp := n
	attrs := map[string]string{}
	for k, v := range n.Attrs {
		attrs[k] = v
	}
	cp.Attrs = attrs
	s.nodes[cp.ID] = &cp
	if err := s.persistNodeLocked(&cp); err != nil {
		return nil, err
	}
	out := cp
	out.Attrs = map[string]string{}
	for k, v := range attrs {
		out.Attrs[k] = v
	}
	return &out, nil
}

func (s *Store) GetNode(id string) (*Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.nodes[id]
	if !ok {
		return nil, fmt.Errorf("node %q not found", id)
	}
	return cloneNode(n), nil
}

func (s *Store) DeleteNode(id string, cascade bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[id]; !ok {
		return fmt.Errorf("node %q not found", id)
	}
	if cascade {
		for _, eid := range append(append([]string{}, s.out[id]...), s.in[id]...) {
			s.deleteEdgeLocked(eid)
		}
	} else if len(s.out[id]) > 0 || len(s.in[id]) > 0 {
		return fmt.Errorf("node %q has edges; set cascade_edges", id)
	}
	delete(s.nodes, id)
	delete(s.out, id)
	delete(s.in, id)
	return s.deleteNodeDBLocked(id)
}

func (s *Store) Search(query, kind string, limit int) []*Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 {
		limit = 50
	}
	q := strings.ToLower(strings.TrimSpace(query))
	out := make([]*Node, 0)
	for _, n := range s.nodes {
		if kind != "" && !strings.EqualFold(n.Kind, kind) {
			continue
		}
		if q != "" && !strings.Contains(strings.ToLower(n.Title), q) &&
			!strings.Contains(strings.ToLower(n.ExternalID), q) {
			continue
		}
		out = append(out, cloneNode(n))
		if len(out) >= limit {
			break
		}
	}
	return out
}

func (s *Store) Link(fromID, toID, rel string, weight float64) (*Edge, error) {
	if rel == "" {
		rel = "related_to"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[fromID]; !ok {
		return nil, fmt.Errorf("from node %q not found", fromID)
	}
	if _, ok := s.nodes[toID]; !ok {
		return nil, fmt.Errorf("to node %q not found", toID)
	}
	if existing := s.findEdgeLocked(fromID, toID, rel); existing != nil {
		if weight > 0 && weight != existing.Weight {
			existing.Weight = weight
			if err := s.persistEdgeLocked(existing); err != nil {
				return nil, err
			}
		}
		cp := *existing
		return &cp, nil
	}
	e := Edge{
		ID: "ge_" + uuid.NewString()[:8], FromID: fromID, ToID: toID, Rel: rel, Weight: weight,
	}
	if e.Weight == 0 {
		e.Weight = 1
	}
	s.edges[e.ID] = &e
	s.out[fromID] = append(s.out[fromID], e.ID)
	s.in[toID] = append(s.in[toID], e.ID)
	if err := s.persistEdgeLocked(&e); err != nil {
		return nil, err
	}
	cp := e
	return &cp, nil
}

func (s *Store) findEdgeLocked(fromID, toID, rel string) *Edge {
	for _, eid := range append(append([]string{}, s.out[fromID]...), s.in[fromID]...) {
		e := s.edges[eid]
		if !strings.EqualFold(e.Rel, rel) {
			continue
		}
		if (e.FromID == fromID && e.ToID == toID) || (e.FromID == toID && e.ToID == fromID) {
			return e
		}
	}
	return nil
}

func (s *Store) Unlink(edgeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.edges[edgeID]; !ok {
		return fmt.Errorf("edge %q not found", edgeID)
	}
	s.deleteEdgeLocked(edgeID)
	return nil
}

func (s *Store) deleteEdgeLocked(edgeID string) {
	e, ok := s.edges[edgeID]
	if !ok {
		return
	}
	s.out[e.FromID] = removeID(s.out[e.FromID], edgeID)
	s.in[e.ToID] = removeID(s.in[e.ToID], edgeID)
	delete(s.edges, edgeID)
	_ = s.deleteEdgeDBLocked(edgeID)
}

func removeID(ids []string, id string) []string {
	out := ids[:0]
	for _, x := range ids {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}

func (s *Store) Neighbors(id, rel string, depth int) (*Node, []Edge, []Node, error) {
	if depth <= 0 {
		depth = 1
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.nodes[id]
	if !ok {
		return nil, nil, nil, fmt.Errorf("node %q not found", id)
	}
	seenN := map[string]struct{}{id: {}}
	seenE := map[string]struct{}{}
	frontier := []string{id}
	var edges []Edge
	var nodes []Node
	for d := 0; d < depth; d++ {
		next := make([]string, 0)
		for _, cur := range frontier {
			for _, eid := range append(append([]string{}, s.out[cur]...), s.in[cur]...) {
				if _, ok := seenE[eid]; ok {
					continue
				}
				e := s.edges[eid]
				if rel != "" && !strings.EqualFold(e.Rel, rel) {
					continue
				}
				seenE[eid] = struct{}{}
				edges = append(edges, *e)
				other := e.ToID
				if other == cur {
					other = e.FromID
				}
				if _, ok := seenN[other]; !ok {
					seenN[other] = struct{}{}
					if on, ok := s.nodes[other]; ok {
						nodes = append(nodes, *cloneNode(on))
						next = append(next, other)
					}
				}
			}
		}
		frontier = next
	}
	return cloneNode(n), edges, nodes, nil
}

func (s *Store) Path(fromID, toID string, maxDepth int) ([]string, []Edge, bool, error) {
	if maxDepth <= 0 {
		maxDepth = 6
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.nodes[fromID]; !ok {
		return nil, nil, false, fmt.Errorf("from node %q not found", fromID)
	}
	if _, ok := s.nodes[toID]; !ok {
		return nil, nil, false, fmt.Errorf("to node %q not found", toID)
	}
	type step struct {
		id   string
		via  string // edge id
		prev int
	}
	queue := []step{{id: fromID, prev: -1}}
	visited := map[string]int{fromID: 0}
	parent := []step{{id: fromID, prev: -1}}
	foundAt := -1
	for i := 0; i < len(queue); i++ {
		cur := queue[i]
		depth := visited[cur.id]
		if cur.id == toID {
			foundAt = i
			break
		}
		if depth >= maxDepth {
			continue
		}
		for _, eid := range append(append([]string{}, s.out[cur.id]...), s.in[cur.id]...) {
			e := s.edges[eid]
			other := e.ToID
			if other == cur.id {
				other = e.FromID
			}
			if _, ok := visited[other]; ok {
				continue
			}
			visited[other] = depth + 1
			parent = append(parent, step{id: other, via: eid, prev: i})
			queue = append(queue, step{id: other, via: eid, prev: i})
		}
	}
	if foundAt < 0 {
		// also check if last queued hit - re-scan
		for i, st := range queue {
			if st.id == toID {
				foundAt = i
				break
			}
		}
	}
	if foundAt < 0 {
		return nil, nil, false, nil
	}
	// reconstruct via parent indices matching queue order
	// Rebuild: find parent entry for toID
	idx := -1
	for i, p := range parent {
		if p.id == toID {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil, nil, false, nil
	}
	var nodeIDs []string
	var edgeIDs []string
	for idx >= 0 {
		p := parent[idx]
		nodeIDs = append([]string{p.id}, nodeIDs...)
		if p.via != "" {
			edgeIDs = append([]string{p.via}, edgeIDs...)
		}
		idx = p.prev
	}
	edges := make([]Edge, 0, len(edgeIDs))
	for _, eid := range edgeIDs {
		edges = append(edges, *s.edges[eid])
	}
	return nodeIDs, edges, true, nil
}

// FindByAttr returns the first node whose attrs[key] equals value.
func (s *Store) FindByAttr(key, value string) *Node {
	if key == "" || value == "" {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, n := range s.nodes {
		if n.Attrs != nil && n.Attrs[key] == value {
			return cloneNode(n)
		}
	}
	return nil
}

func (s *Store) ListNodes() []*Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		out = append(out, cloneNode(n))
	}
	return out
}

func (s *Store) ListEdges() []Edge {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Edge, 0, len(s.edges))
	for _, e := range s.edges {
		out = append(out, *e)
	}
	return out
}

func (s *Store) EdgeCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.edges)
}

func (s *Store) NodesByKind(kind string) []*Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Node, 0)
	for _, n := range s.nodes {
		if kind == "" || strings.EqualFold(n.Kind, kind) {
			out = append(out, cloneNode(n))
		}
	}
	return out
}

func (s *Store) PingDB(ctx context.Context) error {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	if db == nil {
		return fmt.Errorf("database not open")
	}
	var one int
	return db.QueryRowContext(ctx, `SELECT 1`).Scan(&one)
}

func cloneNode(n *Node) *Node {
	cp := *n
	cp.Attrs = map[string]string{}
	for k, v := range n.Attrs {
		cp.Attrs[k] = v
	}
	return &cp
}
