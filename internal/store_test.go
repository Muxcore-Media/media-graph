package internal_test

import (
	"testing"

	"github.com/Muxcore-Media/media-graph/internal"
)

func TestCrossMediaLinkAndNeighbors(t *testing.T) {
	s := internal.NewStore()
	movie, err := s.UpsertNode(internal.Node{Kind: "movie", Title: "Dune", ExternalID: "tmdb:438631"})
	if err != nil {
		t.Fatal(err)
	}
	book, err := s.UpsertNode(internal.Node{Kind: "book", Title: "Dune", ExternalID: "isbn:9780441172719"})
	if err != nil {
		t.Fatal(err)
	}
	album, err := s.UpsertNode(internal.Node{Kind: "album", Title: "Dune Soundtrack"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Link(movie.ID, book.ID, "adaptation_of", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Link(album.ID, movie.ID, "soundtrack_of", 1); err != nil {
		t.Fatal(err)
	}
	_, edges, nodes, err := s.Neighbors(movie.ID, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(edges) != 2 || len(nodes) != 2 {
		t.Fatalf("edges=%d nodes=%d", len(edges), len(nodes))
	}
	path, _, found, err := s.Path(album.ID, book.ID, 4)
	if err != nil || !found {
		t.Fatalf("path err=%v found=%v", err, found)
	}
	if len(path) < 3 {
		t.Fatalf("path=%v", path)
	}
}
