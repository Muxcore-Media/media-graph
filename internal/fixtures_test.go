package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

func TestDuneFranchiseRelatedDepthAndLimit(t *testing.T) {
	m := newFixtureModule(t)
	if _, err := m.IngestLibraryFixtures(filepath.Join("testdata", "library")); err != nil {
		t.Fatal(err)
	}
	dune := m.store.FindByExternalID("tmdb:movie:438631")
	if dune == nil {
		t.Fatal("missing Dune")
	}
	_, related, err := m.store.RelatedTitles(dune.ID, "", "same_franchise", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(related) != 2 {
		t.Fatalf("limit=2 expected 2 related, got %d", len(related))
	}
	if related[0].Weight < related[1].Weight {
		t.Fatalf("expected weight-desc order: %+v", related)
	}
	titles := []string{related[0].Node.Title, related[1].Node.Title}
	if !containsAll(titles, "Dune: Part Two", "Dune: Prophecy") {
		t.Fatalf("expected franchise neighbors, got %v", titles)
	}
}

func containsAll(haystack []string, needles ...string) bool {
	for _, n := range needles {
		found := false
		for _, h := range haystack {
			if h == n {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func graphRequest(method, target string, body *bytes.Reader) *http.Request {
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, target, body)
	} else {
		req = httptest.NewRequest(method, target, nil)
	}
	req.RemoteAddr = "127.0.0.1:12345"
	return req
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

	byExt, err := srv.GetNode(t.Context(), &mgv1.GetNodeRequest{ExternalId: "tmdb:movie:438631"})
	if err != nil {
		t.Fatal(err)
	}
	if byExt.GetNode().GetTitle() != "Dune" {
		t.Fatalf("got %q", byExt.GetNode().GetTitle())
	}

	_, err = srv.GetNode(t.Context(), &mgv1.GetNodeRequest{ExternalId: "missing:999"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}

	byExtRel, err := srv.GetRelatedTitles(t.Context(), &mgv1.GetRelatedTitlesRequest{
		ExternalId: "tmdb:movie:438631", Depth: 1, Limit: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(byExtRel.GetRelated()) < 2 {
		t.Fatalf("Dune related via external_id: %d", len(byExtRel.GetRelated()))
	}

	mux := http.NewServeMux()
	m.registerAdminRoutes(mux)

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, graphRequest(http.MethodGet, "/api/graph", nil))
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
	if edges, ok := summary["edges"].(float64); !ok || edges < 4 {
		t.Fatalf("expected edge count, got %v", summary["edges"])
	}

	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, graphRequest(http.MethodGet, "/api/graph/related?external_id=tmdb:tv:1396", nil))
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
	mux.ServeHTTP(rr, graphRequest(http.MethodGet, "/api/graph/search?q=Dune&kind=movie", nil))
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
	mux.ServeHTTP(rr, graphRequest(http.MethodGet, "/api/graph/nodes?kind=series", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("nodes %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, graphRequest(http.MethodGet, "/api/graph/node?external_id=tmdb:movie:550", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("node %d %s", rr.Code, rr.Body.String())
	}

	dune := m.store.FindByExternalID("tmdb:movie:438631")
	partTwo := m.store.FindByExternalID("tmdb:movie:693134")
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, graphRequest(http.MethodGet, "/api/graph/neighbors?id="+dune.ID, nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("neighbors %d %s", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, graphRequest(http.MethodGet, "/api/graph/path?from_id="+dune.ID+"&to_id="+partTwo.ID, nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("path %d %s", rr.Code, rr.Body.String())
	}
	var pathBody struct {
		Found bool `json:"found"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &pathBody); err != nil || !pathBody.Found {
		t.Fatalf("path not found: %s", rr.Body.String())
	}

	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, graphRequest(http.MethodGet, "/api/graph/edges", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("edges %d", rr.Code)
	}

	nodeBody, _ := json.Marshal(map[string]any{"node": Node{Kind: "person", Title: "Test Person"}})
	rr = httptest.NewRecorder()
	req := graphRequest(http.MethodPost, "/api/graph/node", bytes.NewReader(nodeBody))
	req.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("post node %d %s", rr.Code, rr.Body.String())
	}

	linkBody, _ := json.Marshal(map[string]any{"from_id": dune.ID, "to_id": partTwo.ID, "rel": "sequel_of", "weight": 2})
	rr = httptest.NewRecorder()
	req = graphRequest(http.MethodPost, "/api/graph/link", bytes.NewReader(linkBody))
	req.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("post link %d %s", rr.Code, rr.Body.String())
	}
}

func TestGraphAuthRequiresTokenOffLoopback(t *testing.T) {
	m := newFixtureModule(t)
	m.adminToken = "secret"
	mux := http.NewServeMux()
	m.registerAdminRoutes(mux)

	rr := httptest.NewRecorder()
	req := graphRequest(http.MethodGet, "/api/graph", nil)
	req.RemoteAddr = "10.0.0.1:9999"
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	req = graphRequest(http.MethodGet, "/api/graph", nil)
	req.Header.Set("Authorization", "Bearer secret")
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
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
	if err := m.store.OpenDB(context.Background(), m.dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.store.Close() })
	return m
}
