package internal_test

import (
	"path/filepath"
	"testing"

	"github.com/Muxcore-Media/media-graph/internal"
)

func TestSQLitePersistenceAndAutoLink(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "graph.db")

	s1 := internal.NewStore()
	if err := s1.OpenDB(dbPath); err != nil {
		t.Fatal(err)
	}
	movie, _, err := s1.UpsertNodeWithAutoLink(internal.Node{Kind: "movie", Title: "Dune", ExternalID: "tmdb:438631"}, true)
	if err != nil {
		t.Fatal(err)
	}
	book, linked, err := s1.UpsertNodeWithAutoLink(internal.Node{Kind: "book", Title: "Dune", ExternalID: "isbn:9780441172719"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(linked) != 1 || linked[0].Rel != "same_franchise" {
		t.Fatalf("linked=%+v", linked)
	}
	_ = s1.Close()

	s2 := internal.NewStore()
	if err := s2.OpenDB(dbPath); err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, err := s2.GetNode(movie.ID)
	if err != nil || got.Title != "Dune" {
		t.Fatalf("reload movie: %+v err=%v", got, err)
	}
	_, edges, nodes, err := s2.Neighbors(book.ID, "same_franchise", 1)
	if err != nil || len(edges) != 1 || len(nodes) != 1 {
		t.Fatalf("neighbors edges=%d nodes=%d err=%v", len(edges), len(nodes), err)
	}
}
