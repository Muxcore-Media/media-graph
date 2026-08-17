package internal

import "fmt"

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
	n, edges, nodes, err := s.Neighbors(id, rel, depth)
	if err != nil {
		return nil, nil, err
	}
	byID := map[string]Node{}
	for _, nn := range nodes {
		byID[nn.ID] = nn
	}
	out := make([]RelatedTitle, 0, len(nodes))
	seen := map[string]struct{}{}
	for _, e := range edges {
		other := e.ToID
		if other == id {
			other = e.FromID
		}
		if other == id {
			continue
		}
		if _, ok := seen[other]; ok {
			continue
		}
		nn, ok := byID[other]
		if !ok {
			continue
		}
		seen[other] = struct{}{}
		out = append(out, RelatedTitle{Node: nn, Rel: e.Rel, Weight: e.Weight})
		if len(out) >= limit {
			break
		}
	}
	return n, out, nil
}
