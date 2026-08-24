package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// LibraryFixtures is a local movies/TV dump for offline graph ingest.
type LibraryFixtures struct {
	Movies []FixtureMovie  `json:"movies"`
	Series []FixtureSeries `json:"series"`
	Edges  []FixtureEdge   `json:"edges"`
}

type FixtureMovie struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	IMDBID string `json:"imdb_id"`
	TMDBID int32  `json:"tmdb_id"`
	Year   int32  `json:"year"`
}

type FixtureSeries struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	TMDBID int32  `json:"tmdb_id"`
	Year   int32  `json:"year"`
}

type FixtureEdge struct {
	FromExternalID string  `json:"from_external_id"`
	ToExternalID   string  `json:"to_external_id"`
	Rel            string  `json:"rel"`
	Weight         float64 `json:"weight"`
}

// LoadLibraryFixtures reads a JSON fixture file (or directory containing movies_tv.json).
func LoadLibraryFixtures(path string) (*LibraryFixtures, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	file := path
	if info.IsDir() {
		file = filepath.Join(path, "movies_tv.json")
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var f LibraryFixtures
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parse fixtures: %w", err)
	}
	if len(f.Movies) == 0 && len(f.Series) == 0 {
		return nil, fmt.Errorf("fixtures empty: %s", file)
	}
	return &f, nil
}

// IngestResult counts nodes/edges created from fixtures.
type IngestResult struct {
	Movies int
	Series int
	Edges  int
}

// IngestLibraryFixtures upserts movie/TV nodes and explicit edges from local fixtures.
func (m *Module) IngestLibraryFixtures(path string) (*IngestResult, error) {
	f, err := LoadLibraryFixtures(path)
	if err != nil {
		return nil, err
	}
	res := &IngestResult{}
	for _, mv := range f.Movies {
		attrs := map[string]string{
			"movie_id": mv.ID,
			"tmdb_id":  strconv.FormatInt(int64(mv.TMDBID), 10),
			"year":     strconv.FormatInt(int64(mv.Year), 10),
			"imdb_id":  mv.IMDBID,
			"source":   "fixture",
		}
		if _, err := m.upsertLibraryNode("movie", mv.Title, movieExternalID(mv.TMDBID), attrs); err != nil {
			return res, err
		}
		res.Movies++
	}
	for _, tv := range f.Series {
		attrs := map[string]string{
			"series_id": tv.ID,
			"tmdb_id":   strconv.FormatInt(int64(tv.TMDBID), 10),
			"year":      strconv.FormatInt(int64(tv.Year), 10),
			"source":    "fixture",
		}
		if _, err := m.upsertLibraryNode("series", tv.Name, seriesExternalID(tv.TMDBID), attrs); err != nil {
			return res, err
		}
		res.Series++
	}
	for _, e := range f.Edges {
		from := m.store.FindByExternalID(e.FromExternalID)
		to := m.store.FindByExternalID(e.ToExternalID)
		if from == nil || to == nil {
			continue
		}
		rel := e.Rel
		if rel == "" {
			rel = "related_to"
		}
		if edgeExists(m.store, from.ID, to.ID, rel) {
			continue
		}
		if _, err := m.store.Link(from.ID, to.ID, rel, e.Weight); err != nil {
			return res, err
		}
		res.Edges++
	}
	return res, nil
}

func edgeExists(s *Store, fromID, toID, rel string) bool {
	_, edges, _, err := s.Neighbors(fromID, rel, 1)
	if err != nil {
		return false
	}
	for _, e := range edges {
		if (e.FromID == fromID && e.ToID == toID) || (e.FromID == toID && e.ToID == fromID) {
			return true
		}
	}
	return false
}
