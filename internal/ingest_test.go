package internal

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Muxcore-Media/core/pkg/contracts"
	abv1 "github.com/Muxcore-Media/media-audiobooks/proto/gen/muxcore/audiobooks/v1"
	booksv1 "github.com/Muxcore-Media/media-books/proto/gen/muxcore/books/v1"
	comicsv1 "github.com/Muxcore-Media/media-comics/proto/gen/muxcore/comics/v1"
	mgmntv1 "github.com/Muxcore-Media/media-movies/proto/mgmntv1"
	musicv1 "github.com/Muxcore-Media/media-music/proto/gen/muxcore/music/v1"
	tvmgmtv1 "github.com/Muxcore-Media/media-tvshows/proto/tvmgmtv1"
)

const testBufSize = 1 << 20

type stubBooks struct {
	booksv1.UnimplementedBookManagementServiceServer
}

func (stubBooks) ListBooks(context.Context, *booksv1.ListBooksRequest) (*booksv1.ListBooksResponse, error) {
	return &booksv1.ListBooksResponse{Books: []*booksv1.Book{{
		Id: "bk_1", AuthorId: "au_1", Title: "Dune", Isbn: "9780441172719", Year: 1965,
	}}}, nil
}

type stubMusic struct {
	musicv1.UnimplementedMusicManagementServiceServer
}

func (stubMusic) ListArtists(context.Context, *musicv1.ListArtistsRequest) (*musicv1.ListArtistsResponse, error) {
	return &musicv1.ListArtistsResponse{Artists: []*musicv1.Artist{{
		Id: "ar_1", Name: "Hans Zimmer",
	}}}, nil
}

func (stubMusic) ListAlbums(context.Context, *musicv1.ListAlbumsRequest) (*musicv1.ListAlbumsResponse, error) {
	return &musicv1.ListAlbumsResponse{Albums: []*musicv1.Album{{
		Id: "al_1", ArtistId: "ar_1", Title: "Dune Soundtrack",
		MusicbrainzId: "mbid-dune-ost", Year: 2021,
	}}}, nil
}

type stubComics struct {
	comicsv1.UnimplementedComicManagementServiceServer
}

func (stubComics) ListSeries(context.Context, *comicsv1.ListSeriesRequest) (*comicsv1.ListSeriesResponse, error) {
	return &comicsv1.ListSeriesResponse{Series: []*comicsv1.Series{{
		Id: "cm_1", Title: "Dune", ComicvineId: "12345", Publisher: "Marvel",
	}}}, nil
}

type stubAudiobooks struct {
	abv1.UnimplementedAudiobookManagementServiceServer
}

func (stubAudiobooks) ListAudiobooks(context.Context, *abv1.ListAudiobooksRequest) (*abv1.ListAudiobooksResponse, error) {
	return &abv1.ListAudiobooksResponse{Audiobooks: []*abv1.Audiobook{{
		Id: "ab_1", AuthorId: "au_ab", Title: "Dune", Asin: "B00DUNE001", Year: 2007,
	}}}, nil
}

func startBufGRPC(t *testing.T, register func(*grpc.Server)) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(testBufSize)
	srv := grpc.NewServer()
	register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop() })
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

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
	if err := m.store.OpenDB(context.Background(), m.dbPath); err != nil {
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

func newIngestTestModule(t *testing.T) *Module {
	t.Helper()
	m := NewModule(Config{
		DBPath:        filepath.Join(t.TempDir(), "g.db"),
		IngestEnabled: false,
		AutoLink:      false,
	})
	if err := m.store.OpenDB(context.Background(), m.dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.store.Close() })
	return m
}

func TestCrossMediaExternalIDs(t *testing.T) {
	if got := bookExternalID("9780441172719"); got != "isbn:9780441172719" {
		t.Fatalf("book: %q", got)
	}
	if got := albumExternalID("mbid-1"); got != "mbid:mbid-1" {
		t.Fatalf("album: %q", got)
	}
	if got := comicExternalID("999"); got != "comicvine:999" {
		t.Fatalf("comic: %q", got)
	}
	if got := audiobookExternalID("B001"); got != "asin:B001" {
		t.Fatalf("audiobook: %q", got)
	}
}

func TestIngestBooksMusicComicsAudiobooksBufconn(t *testing.T) {
	m := newIngestTestModule(t)

	booksConn := startBufGRPC(t, func(s *grpc.Server) {
		booksv1.RegisterBookManagementServiceServer(s, stubBooks{})
	})
	musicConn := startBufGRPC(t, func(s *grpc.Server) {
		musicv1.RegisterMusicManagementServiceServer(s, stubMusic{})
	})
	comicsConn := startBufGRPC(t, func(s *grpc.Server) {
		comicsv1.RegisterComicManagementServiceServer(s, stubComics{})
	})
	abConn := startBufGRPC(t, func(s *grpc.Server) {
		abv1.RegisterAudiobookManagementServiceServer(s, stubAudiobooks{})
	})

	m.peerMu.Lock()
	m.booksClient = booksv1.NewBookManagementServiceClient(booksConn)
	m.musicClient = musicv1.NewMusicManagementServiceClient(musicConn)
	m.comicsClient = comicsv1.NewComicManagementServiceClient(comicsConn)
	m.audiobooksClient = abv1.NewAudiobookManagementServiceClient(abConn)
	m.peerMu.Unlock()

	ctx := context.Background()
	if n, err := m.ingestBooks(ctx); err != nil || n != 1 {
		t.Fatalf("books n=%d err=%v", n, err)
	}
	if n, err := m.ingestAlbums(ctx); err != nil || n != 1 {
		t.Fatalf("albums n=%d err=%v", n, err)
	}
	if n, err := m.ingestComics(ctx); err != nil || n != 1 {
		t.Fatalf("comics n=%d err=%v", n, err)
	}
	if n, err := m.ingestAudiobooks(ctx); err != nil || n != 1 {
		t.Fatalf("audiobooks n=%d err=%v", n, err)
	}

	if m.store.FindByExternalID("isbn:9780441172719") == nil {
		t.Fatal("missing book node")
	}
	if m.store.FindByExternalID("mbid:mbid-dune-ost") == nil {
		t.Fatal("missing album node")
	}
	if m.store.FindByExternalID("comicvine:12345") == nil {
		t.Fatal("missing comic node")
	}
	if m.store.FindByExternalID("asin:B00DUNE001") == nil {
		t.Fatal("missing audiobook node")
	}
}

func TestIngestMoviesPrunesRemoved(t *testing.T) {
	m := newIngestTestModule(t)
	moviesConn := startBufGRPC(t, func(s *grpc.Server) {
		mgmntv1.RegisterMovieManagementServiceServer(s, stubMovies{})
	})
	m.peerMu.Lock()
	m.moviesClient = mgmntv1.NewMovieManagementServiceClient(moviesConn)
	m.peerMu.Unlock()

	if _, err := m.upsertLibraryNode("movie", "Old Title", "tmdb:movie:1", map[string]string{"movie_id": "mv_old"}); err != nil {
		t.Fatal(err)
	}
	if n, err := m.ingestMovies(context.Background()); err != nil || n != 1 {
		t.Fatalf("ingest n=%d err=%v", n, err)
	}
	if m.store.FindByAttr("movie_id", "mv_old") != nil {
		t.Fatal("stale movie should be pruned")
	}
	if m.store.FindByExternalID("tmdb:movie:550") == nil {
		t.Fatal("expected ingested movie")
	}
}

type stubMovies struct {
	mgmntv1.UnimplementedMovieManagementServiceServer
}

func (stubMovies) ListMovies(context.Context, *mgmntv1.ListMoviesRequest) (*mgmntv1.ListMoviesResponse, error) {
	return &mgmntv1.ListMoviesResponse{Movies: []*mgmntv1.MovieItem{{
		Id: "mv_1", Title: "Fight Club", TmdbId: 550, Year: 1999,
	}}}, nil
}

type stubTV struct {
	tvmgmtv1.UnimplementedTvManagementServiceServer
}

func (stubTV) ListTVShows(context.Context, *tvmgmtv1.ListTVShowsRequest) (*tvmgmtv1.ListTVShowsResponse, error) {
	return &tvmgmtv1.ListTVShowsResponse{Series: []*tvmgmtv1.TVSeries{{
		Id: "tv_1", Name: "Breaking Bad", TmdbId: 1396, Year: 2008,
	}}}, nil
}

func TestLinkIdempotentBothDirections(t *testing.T) {
	s := NewStore()
	a, err := s.UpsertNode(Node{Kind: "movie", Title: "A"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.UpsertNode(Node{Kind: "movie", Title: "B"})
	if err != nil {
		t.Fatal(err)
	}
	e1, err := s.Link(a.ID, b.ID, "related_to", 1)
	if err != nil {
		t.Fatal(err)
	}
	e2, err := s.Link(b.ID, a.ID, "related_to", 1)
	if err != nil {
		t.Fatal(err)
	}
	if e1.ID != e2.ID {
		t.Fatalf("expected same edge id %s vs %s", e1.ID, e2.ID)
	}
	if s.EdgeCount() != 1 {
		t.Fatalf("edges=%d", s.EdgeCount())
	}
}

func TestHealthRequiresSQLite(t *testing.T) {
	m := NewModule(Config{IngestEnabled: false})
	if err := m.Health(context.Background()); err == nil {
		t.Fatal("expected health failure without db")
	}
}

func TestNewModuleRespectsConfigDefaults(t *testing.T) {
	m := NewModule(Config{AutoLink: false, IngestEnabled: false})
	if m.autoLink {
		t.Fatal("autoLink should stay false")
	}
	if m.ingestEnabled {
		t.Fatal("ingestEnabled should stay false")
	}
}

func TestUpdateSettingStartsIngest(t *testing.T) {
	m := NewModule(Config{IngestEnabled: false, AutoLink: false, GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0"})
	if err := m.UpdateSetting("ingest_enabled", "true"); err != nil {
		t.Fatal(err)
	}
	deadline := timeAfter(2 * time.Second)
	for {
		m.ingestMu.Lock()
		running := m.ingestCancel != nil
		m.ingestMu.Unlock()
		if running {
			break
		}
		if deadline() {
			t.Fatal("ingest loops did not start")
		}
		timeSleep(20 * time.Millisecond)
	}
	m.stopIngest()
}

// timeAfter and timeSleep allow stubbing in tests if needed.
var timeSleep = func(d time.Duration) { time.Sleep(d) }

func timeAfter(d time.Duration) func() bool {
	deadline := time.Now().Add(d)
	return func() bool { return time.Now().After(deadline) }
}
