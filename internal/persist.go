package internal

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func (s *Store) OpenDB(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		_ = db.Close()
		return fmt.Errorf("wal: %w", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS nodes (
			id TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			title TEXT NOT NULL,
			external_id TEXT DEFAULT '',
			attrs TEXT DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS edges (
			id TEXT PRIMARY KEY,
			from_id TEXT NOT NULL,
			to_id TEXT NOT NULL,
			rel TEXT NOT NULL,
			weight REAL DEFAULT 1,
			FOREIGN KEY (from_id) REFERENCES nodes(id) ON DELETE CASCADE,
			FOREIGN KEY (to_id) REFERENCES nodes(id) ON DELETE CASCADE
		);
		CREATE INDEX IF NOT EXISTS idx_nodes_title ON nodes(title);
		CREATE INDEX IF NOT EXISTS idx_nodes_ext ON nodes(external_id);
		CREATE INDEX IF NOT EXISTS idx_edges_from ON edges(from_id);
		CREATE INDEX IF NOT EXISTS idx_edges_to ON edges(to_id);
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("schema: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db = db
	return s.loadLocked()
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

func (s *Store) loadLocked() error {
	s.nodes = map[string]*Node{}
	s.edges = map[string]*Edge{}
	s.out = map[string][]string{}
	s.in = map[string][]string{}

	rows, err := s.db.Query(`SELECT id, kind, title, external_id, attrs FROM nodes`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var n Node
		var attrsJSON string
		if err := rows.Scan(&n.ID, &n.Kind, &n.Title, &n.ExternalID, &attrsJSON); err != nil {
			return err
		}
		n.Attrs = map[string]string{}
		_ = json.Unmarshal([]byte(attrsJSON), &n.Attrs)
		cp := n
		s.nodes[cp.ID] = &cp
	}
	if err := rows.Err(); err != nil {
		return err
	}

	erows, err := s.db.Query(`SELECT id, from_id, to_id, rel, weight FROM edges`)
	if err != nil {
		return err
	}
	defer erows.Close()
	for erows.Next() {
		var e Edge
		if err := erows.Scan(&e.ID, &e.FromID, &e.ToID, &e.Rel, &e.Weight); err != nil {
			return err
		}
		cp := e
		s.edges[cp.ID] = &cp
		s.out[cp.FromID] = append(s.out[cp.FromID], cp.ID)
		s.in[cp.ToID] = append(s.in[cp.ToID], cp.ID)
	}
	return erows.Err()
}

func (s *Store) persistNodeLocked(n *Node) error {
	if s.db == nil {
		return nil
	}
	b, _ := json.Marshal(n.Attrs)
	_, err := s.db.Exec(
		`INSERT INTO nodes(id, kind, title, external_id, attrs) VALUES(?,?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET kind=excluded.kind, title=excluded.title,
		 external_id=excluded.external_id, attrs=excluded.attrs`,
		n.ID, n.Kind, n.Title, n.ExternalID, string(b),
	)
	return err
}

func (s *Store) deleteNodeDBLocked(id string) error {
	if s.db == nil {
		return nil
	}
	_, err := s.db.Exec(`DELETE FROM nodes WHERE id=?`, id)
	return err
}

func (s *Store) persistEdgeLocked(e *Edge) error {
	if s.db == nil {
		return nil
	}
	_, err := s.db.Exec(
		`INSERT INTO edges(id, from_id, to_id, rel, weight) VALUES(?,?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET from_id=excluded.from_id, to_id=excluded.to_id,
		 rel=excluded.rel, weight=excluded.weight`,
		e.ID, e.FromID, e.ToID, e.Rel, e.Weight,
	)
	return err
}

func (s *Store) deleteEdgeDBLocked(id string) error {
	if s.db == nil {
		return nil
	}
	_, err := s.db.Exec(`DELETE FROM edges WHERE id=?`, id)
	return err
}

func normalizeTitle(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	t = strings.Join(strings.Fields(t), " ")
	return t
}

// UpsertNodeWithAutoLink upserts and links same-title nodes across kinds.
func (s *Store) UpsertNodeWithAutoLink(n Node, autoLink bool) (*Node, []Edge, error) {
	out, err := s.UpsertNode(n)
	if err != nil {
		return nil, nil, err
	}
	if !autoLink {
		return out, nil, nil
	}
	norm := normalizeTitle(out.Title)
	if norm == "" {
		return out, nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var linked []Edge
	for _, other := range s.nodes {
		if other.ID == out.ID {
			continue
		}
		if normalizeTitle(other.Title) != norm {
			continue
		}
		if strings.EqualFold(other.Kind, out.Kind) {
			continue
		}
		// skip if edge already exists either direction with same_franchise
		exists := false
		for _, eid := range append(append([]string{}, s.out[out.ID]...), s.in[out.ID]...) {
			e := s.edges[eid]
			if e.Rel != "same_franchise" {
				continue
			}
			if (e.FromID == out.ID && e.ToID == other.ID) || (e.FromID == other.ID && e.ToID == out.ID) {
				exists = true
				break
			}
		}
		if exists {
			continue
		}
		e := Edge{
			ID: "ge_" + shortID(), FromID: out.ID, ToID: other.ID,
			Rel: "same_franchise", Weight: 1,
		}
		s.edges[e.ID] = &e
		s.out[e.FromID] = append(s.out[e.FromID], e.ID)
		s.in[e.ToID] = append(s.in[e.ToID], e.ID)
		_ = s.persistEdgeLocked(&e)
		linked = append(linked, e)
	}
	return out, linked, nil
}

func shortID() string {
	return uuid.NewString()[:8]
}

// FindByExternalID returns a node with the given external id, if any.
func (s *Store) FindByExternalID(ext string) *Node {
	if strings.TrimSpace(ext) == "" {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, n := range s.nodes {
		if n.ExternalID == ext {
			return cloneNode(n)
		}
	}
	return nil
}
