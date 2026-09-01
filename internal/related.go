package internal

import (
	"fmt"
	"sort"
	"strings"
)

// RelatedTitle is a neighbor title with the connecting relationship.
type RelatedTitle struct {
	Node   Node
	Rel    string
	Weight float64
}

// RelatedTitles walks the graph from id (or externalID) and returns related titles.
func (s *Store) RelatedTitles(id, externalID, rel string, depth, limit int) (*Node, []RelatedTitle, error) {
	if id == "" && externalID != "" {
		n := s.FindByExternalID(externalID)
		if n == nil {
			return nil, nil, fmt.Errorf("node with external_id %q not found", externalID)
		}
		id = n.ID
	}
	if id == "" {
		return nil, nil, fmt.Errorf("id or external_id required")
	}
	if depth <= 0 {
		depth = 1
	}
	if limit <= 0 {
		limit = 20
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	root, ok := s.nodes[id]
	if !ok {
		return nil, nil, fmt.Errorf("node %q not found", id)
	}

	type best struct {
		node   Node
		rel    string
		weight float64
		depth  int
	}
	found := map[string]best{}
	frontier := []string{id}
	seen := map[string]struct{}{id: {}}

	for d := 1; d <= depth; d++ {
		next := make([]string, 0)
		for _, cur := range frontier {
			for _, eid := range append(append([]string{}, s.out[cur]...), s.in[cur]...) {
				e := s.edges[eid]
				if rel != "" && !strings.EqualFold(e.Rel, rel) {
					continue
				}
				other := e.ToID
				if other == cur {
					other = e.FromID
				}
				if other == id {
					continue
				}
				on, ok := s.nodes[other]
				if !ok {
					continue
				}
				cand := best{
					node:   *cloneNode(on),
					rel:    e.Rel,
					weight: e.Weight,
					depth:  d,
				}
				if cand.weight == 0 {
					cand.weight = 1
				}
				prev, exists := found[other]
				if !exists || cand.weight > prev.weight || (cand.weight == prev.weight && cand.depth < prev.depth) {
					found[other] = cand
				}
				if _, ok := seen[other]; !ok {
					seen[other] = struct{}{}
					next = append(next, other)
				}
			}
		}
		frontier = next
	}

	out := make([]RelatedTitle, 0, len(found))
	for _, b := range found {
		out = append(out, RelatedTitle{Node: b.node, Rel: b.rel, Weight: b.weight})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Weight != out[j].Weight {
			return out[i].Weight > out[j].Weight
		}
		return out[i].Node.Title < out[j].Node.Title
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return cloneNode(root), out, nil
}
