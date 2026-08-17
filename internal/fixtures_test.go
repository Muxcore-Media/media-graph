package internal

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	mgv1 "github.com/Muxcore-Media/media-graph/proto/gen/muxcore/mediagraph/v1"
)

func TestIngestLibraryFixturesOffline(t *testing.T) {
	m := newFixtureModule(t)
	res, err := m.IngestLibraryFixtures(filepath.Join("testdata", "library"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Movies < 4 || res.Series < 4 {
		t.Fatalf("expected movie+tv fixtures, got %+v", res)
	}
	if res.Edges < 4 {
		t.Fatalf("expected explicit fixture edges, got %+v", res)
	}

	dune := m.store.FindByExternalID("tmdb:movie:438631")
	if dune == nil {
		t.Fatal("missing Dune movie node")
	}
	_, related, err := m.store.RelatedTitles(dune.ID, "", "", 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(related) < 2 {
		t.Fatalf("expected Dune related titles, got %d", len(related))
	}

	_, fcRelated, err := m.store.RelatedTitles("", "tmdb:movie:550", "same_franchise", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(fcRelated) < 1 {
		t.Fatal("expected same_franchise auto-link for Fight Club")
	}

	res2, err := m.IngestLibraryFixtures(filepath.Join("testdata", "library"))
	if err != nil {
		t.Fatal(err)
	}
	if res2.Edges != 0 {
		t.Fatalf("expected no new edges on re-ingest, got %+v", res2)
	}
}

func TestGetRelatedTitlesAPIAndAdminHTTP(t *testing.T) {
	m := newFixtureModule(t)
	if _, err := m.IngestLibraryFixtures(filepath.Join("testdata", "library")); err != nil {
		t.Fatal(err)
	}

	bb := m.store.FindByExternalID("tmdb:tv:1396")
	if bb == nil {
		t.Fatal("missing Breaking Bad")
	}
	srv := &graphServer{m: m}
	resp, err := srv.GetRelatedTitles(t.Context(), &mgv1.GetRelatedTitlesRequest{
		Id: bb.ID, Limit: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetRelated()) < 1 {
		t.Fatal("expected Better Call Saul related via gRPC")
	}

	byExt, err := srv.GetRelatedTitles(t.Context(), &mgv1.GetRelatedTitlesRequest{
		ExternalId: "tmdb:movie:438631", Depth: 1, Limit: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(byExt.GetRelated()) < 2 {
		t.Fatalf("Dune related via external_id: %d", len(byExt.GetRelated()))
	}

	mux := http.NewServeMux()
	m.registerAdminRoutes(mux)

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/graph", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("summary status %d", rr.Code)
	}
	var summary map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &summary); err != nil {
		t.Fatal(err)
	}
	if summary["ok"] != true {
		t.Fatalf("%v", summary)
	}

	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/graph/related?external_id=tmdb:tv:1396", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("related status %d body=%s", rr.Code, rr.Body.String())
	}
	var body struct {
		Related []RelatedTitle `json:"related"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Related) < 1 {
		t.Fatalf("admin related empty: %s", rr.Body.String())
	}

	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/graph/search?q=Dune&kind=movie", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("search %d", rr.Code)
	}
	var search struct {
		Nodes []Node `json:"nodes"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &search); err != nil {
		t.Fatal(err)
	}
	if len(search.Nodes) < 1 {
		t.Fatal("expected Dune movie search hits")
	}

	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/graph/nodes?kind=series", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("nodes %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/graph/node?external_id=tmdb:movie:550", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("node %d %s", rr.Code, rr.Body.String())
	}
}

func newFixtureModule(t *testing.T) *Module {
	t.Helper()
	m := NewModule(Config{
		DBPath:        filepath.Join(t.TempDir(), "g.db"),
		IngestEnabled: false,
		AutoLink:      true,
		GRPCAddr:      "127.0.0.1:0",
		HTTPAddr:      "127.0.0.1:0",
	})
	m.ingestEnabled = false
	m.autoLink = true
	if err := m.store.OpenDB(m.dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.store.Close() })
	return m
}
