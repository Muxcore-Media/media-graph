package internal

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/Muxcore-Media/core/pkg/contracts"
)

func TestExternalIDs(t *testing.T) {
	if got := movieExternalID(550); got != "tmdb:movie:550" {
		t.Fatalf("movie: %q", got)
	}
	if got := seriesExternalID(1396); got != "tmdb:tv:1396" {
		t.Fatalf("tv: %q", got)
	}
	if movieExternalID(0) != "" || seriesExternalID(-1) != "" {
		t.Fatal("expected empty for invalid tmdb")
	}
}

func TestApplyLibraryEventUpsertAndRemove(t *testing.T) {
	dir := t.TempDir()
	m := NewModule(Config{
		DBPath:         filepath.Join(dir, "g.db"),
		IngestEnabled:  false,
		AutoLink:       true,
		IngestInterval: 0,
	})
	if err := m.store.OpenDB(m.dbPath); err != nil {
		t.Fatal(err)
	}
	defer m.store.Close()

	payload, _ := json.Marshal(contracts.MovieAddedPayload{
		Title: "Fight Club", MovieID: "mv_1", TMDBID: 550, Year: 1999,
	})
	m.applyLibraryEvent(contracts.EventMovieAdded, payload)

	n := m.store.FindByExternalID("tmdb:movie:550")
	if n == nil || n.Title != "Fight Club" || n.Kind != "movie" {
		t.Fatalf("movie node: %+v", n)
	}
	if n.Attrs["movie_id"] != "mv_1" {
		t.Fatalf("attrs: %+v", n.Attrs)
	}

	tvPayload, _ := json.Marshal(contracts.TVAddedPayload{
		Name: "Fight Club", SeriesID: "tv_1", TMDBID: 999,
	})
	m.applyLibraryEvent(contracts.EventTVAdded, tvPayload)
	linked := m.store.FindByExternalID("tmdb:tv:999")
	if linked == nil {
		t.Fatal("expected tv node")
	}
	_, edges, _, err := m.store.Neighbors(n.ID, "same_franchise", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(edges) == 0 {
		t.Fatal("expected same_franchise auto-link")
	}

	rm, _ := json.Marshal(contracts.MovieRemovedPayload{MovieID: "mv_1", TMDBID: 550, Title: "Fight Club"})
	m.applyLibraryEvent(contracts.EventMovieRemoved, rm)
	if m.store.FindByExternalID("tmdb:movie:550") != nil {
		t.Fatal("movie should be removed")
	}
}
