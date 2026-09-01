package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Muxcore-Media/core/pkg/contracts"
	eventsv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/events/v1"
	"github.com/Muxcore-Media/core/sdk/go/client"
	abv1 "github.com/Muxcore-Media/media-audiobooks/proto/gen/muxcore/audiobooks/v1"
	booksv1 "github.com/Muxcore-Media/media-books/proto/gen/muxcore/books/v1"
	comicsv1 "github.com/Muxcore-Media/media-comics/proto/gen/muxcore/comics/v1"
	mgmntv1 "github.com/Muxcore-Media/media-movies/proto/mgmntv1"
	musicv1 "github.com/Muxcore-Media/media-music/proto/gen/muxcore/music/v1"
	tvmgmtv1 "github.com/Muxcore-Media/media-tvshows/proto/tvmgmtv1"
)

func movieExternalID(tmdbID int32) string {
	if tmdbID <= 0 {
		return ""
	}
	return fmt.Sprintf("tmdb:movie:%d", tmdbID)
}

func seriesExternalID(tmdbID int32) string {
	if tmdbID <= 0 {
		return ""
	}
	return fmt.Sprintf("tmdb:tv:%d", tmdbID)
}

func bookExternalID(isbn string) string {
	isbn = strings.TrimSpace(isbn)
	if isbn == "" {
		return ""
	}
	return "isbn:" + isbn
}

func albumExternalID(mbid string) string {
	mbid = strings.TrimSpace(mbid)
	if mbid == "" {
		return ""
	}
	return "mbid:" + mbid
}

func comicExternalID(comicvineID string) string {
	comicvineID = strings.TrimSpace(comicvineID)
	if comicvineID == "" {
		return ""
	}
	return "comicvine:" + comicvineID
}

func audiobookExternalID(asin string) string {
	asin = strings.TrimSpace(asin)
	if asin == "" {
		return ""
	}
	return "asin:" + asin
}

func (m *Module) startIngest(ctx context.Context) {
	m.ensureIngestLoops(ctx)
}

func (m *Module) ensureIngestLoops(parent context.Context) {
	m.cfgMu.RLock()
	enabled := m.ingestEnabled
	m.cfgMu.RUnlock()

	m.ingestMu.Lock()
	if m.ingestCancel != nil {
		if !enabled {
			cancel := m.ingestCancel
			m.ingestCancel = nil
			m.ingestMu.Unlock()
			cancel()
			m.ingestWG.Wait()
		} else {
			m.ingestMu.Unlock()
		}
		return
	}
	if !enabled {
		m.ingestMu.Unlock()
		slog.Info("media-graph: library ingest disabled (GRAPH_INGEST_ENABLED)")
		return
	}

	ctx, cancel := context.WithCancel(parent)
	m.ingestCancel = cancel
	m.ingestMu.Unlock()
	m.ingestWG.Add(2)
	go func() {
		defer m.ingestWG.Done()
		m.dialCoreAndSubscribe(ctx)
	}()
	go func() {
		defer m.ingestWG.Done()
		m.ingestLoop(ctx)
	}()
}

func (m *Module) stopIngest() {
	m.ingestMu.Lock()
	if m.ingestCancel != nil {
		m.ingestCancel()
		m.ingestCancel = nil
	}
	m.ingestMu.Unlock()
	m.ingestWG.Wait()
}

func (m *Module) dialCoreAndSubscribe(ctx context.Context) {
	backoff := 8 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		if err := m.connectAndSubscribe(ctx); err != nil {
			slog.Error("media-graph: dial core", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				if backoff < 2*time.Minute {
					backoff *= 2
				}
			}
			continue
		}
		<-ctx.Done()
		return
	}
}

func (m *Module) connectAndSubscribe(ctx context.Context) error {
	meshAddr := os.Getenv("MUXCORE_GRPC_ADDR")
	if meshAddr == "" {
		meshAddr = "localhost:9090"
	}
	var opts []client.Option
	if m.grpcInsecure {
		opts = append(opts, client.WithInsecure())
	}
	c, err := client.Dial(meshAddr, opts...)
	if err != nil {
		return err
	}
	m.peerMu.Lock()
	if m.mc != nil {
		_ = m.mc.Close()
	}
	m.mc = c
	m.peerMu.Unlock()
	slog.Info("media-graph: connected to core mesh", "addr", meshAddr)

	for _, et := range []string{
		contracts.EventMovieAdded,
		contracts.EventMovieRemoved,
		contracts.EventMovieUpdated,
		contracts.EventTVAdded,
		contracts.EventTVRemoved,
		contracts.EventTVUpdated,
	} {
		ch, cancel, err := c.Events.Subscribe(ctx, et)
		if err != nil {
			slog.Warn("media-graph: subscribe", "type", et, "error", err)
			continue
		}
		m.eventMu.Lock()
		m.eventCancels = append(m.eventCancels, cancel)
		m.eventMu.Unlock()
		go m.handleIngestEvents(ctx, et, ch, cancel)
		slog.Info("media-graph: subscribed", "type", et)
	}
	return nil
}

func (m *Module) handleIngestEvents(ctx context.Context, eventType string, ch <-chan *eventsv1.Event, cancel context.CancelFunc) {
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			m.applyLibraryEvent(eventType, evt.GetPayload())
		}
	}
}

func (m *Module) applyLibraryEvent(eventType string, payload []byte) {
	switch eventType {
	case contracts.EventMovieAdded, contracts.EventMovieUpdated:
		var p contracts.MovieAddedPayload
		if json.Unmarshal(payload, &p) != nil {
			return
		}
		_, _ = m.upsertLibraryNode("movie", p.Title, movieExternalID(p.TMDBID), map[string]string{
			"movie_id": p.MovieID,
			"tmdb_id":  strconv.FormatInt(int64(p.TMDBID), 10),
			"year":     strconv.FormatInt(int64(p.Year), 10),
		})
	case contracts.EventMovieRemoved:
		var p contracts.MovieRemovedPayload
		if json.Unmarshal(payload, &p) != nil {
			return
		}
		m.removeLibraryNode(movieExternalID(p.TMDBID), "movie_id", p.MovieID)
	case contracts.EventTVAdded, contracts.EventTVUpdated:
		var p contracts.TVAddedPayload
		if json.Unmarshal(payload, &p) != nil {
			return
		}
		_, _ = m.upsertLibraryNode("series", p.Name, seriesExternalID(p.TMDBID), map[string]string{
			"series_id": p.SeriesID,
			"tmdb_id":   strconv.FormatInt(int64(p.TMDBID), 10),
		})
	case contracts.EventTVRemoved:
		var p contracts.TVRemovedPayload
		if json.Unmarshal(payload, &p) != nil {
			return
		}
		m.removeLibraryNode("", "series_id", p.SeriesID)
	}
}

func (m *Module) upsertLibraryNode(kind, title, externalID string, attrs map[string]string) (*Node, error) {
	title = strings.TrimSpace(title)
	if kind == "" || title == "" {
		return nil, fmt.Errorf("kind and title required")
	}
	n := Node{Kind: kind, Title: title, ExternalID: externalID, Attrs: attrs}
	if externalID != "" {
		if existing := m.store.FindByExternalID(externalID); existing != nil {
			n.ID = existing.ID
		}
	}
	m.cfgMu.RLock()
	auto := m.autoLink
	m.cfgMu.RUnlock()
	out, _, err := m.store.UpsertNodeWithAutoLink(n, auto)
	if err != nil {
		slog.Warn("media-graph: ingest upsert", "kind", kind, "title", title, "error", err)
		return nil, err
	}
	return out, nil
}

func (m *Module) removeLibraryNode(externalID, attrKey, attrVal string) {
	var n *Node
	if externalID != "" {
		n = m.store.FindByExternalID(externalID)
	}
	if n == nil && attrKey != "" && attrVal != "" {
		n = m.store.FindByAttr(attrKey, attrVal)
	}
	if n == nil {
		return
	}
	if err := m.store.DeleteNode(n.ID, true); err != nil {
		slog.Warn("media-graph: ingest delete", "id", n.ID, "error", err)
	}
}

func (m *Module) pruneLibraryNodes(kind, attrKey string, seen map[string]struct{}) {
	for _, n := range m.store.NodesByKind(kind) {
		val := ""
		if n.Attrs != nil {
			val = n.Attrs[attrKey]
		}
		if val == "" {
			continue
		}
		if _, ok := seen[val]; !ok {
			if err := m.store.DeleteNode(n.ID, true); err != nil {
				slog.Warn("media-graph: prune delete", "kind", kind, "id", n.ID, "error", err)
			}
		}
	}
}

func (m *Module) ingestLoop(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(12 * time.Second):
	}
	m.ingestLibrariesOnce(ctx)
	for {
		m.cfgMu.RLock()
		enabled := m.ingestEnabled
		interval := m.ingestInterval
		m.cfgMu.RUnlock()
		if !enabled {
			select {
			case <-ctx.Done():
				return
			case <-time.After(30 * time.Second):
				continue
			}
		}
		if interval <= 0 {
			interval = 15 * time.Minute
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			m.ingestLibrariesOnce(ctx)
		}
	}
}

func (m *Module) ingestLibrariesOnce(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	runCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	movies, errM := m.ingestMovies(runCtx)
	tv, errT := m.ingestTV(runCtx)
	books, errB := m.ingestBooks(runCtx)
	albums, errA := m.ingestAlbums(runCtx)
	comics, errC := m.ingestComics(runCtx)
	audiobooks, errAB := m.ingestAudiobooks(runCtx)
	slog.Info("media-graph: library ingest pass",
		"movies", movies, "tv", tv, "books", books, "albums", albums,
		"comics", comics, "audiobooks", audiobooks,
		"movies_err", errM, "tv_err", errT, "books_err", errB,
		"albums_err", errA, "comics_err", errC, "audiobooks_err", errAB)
}

func (m *Module) ingestMovies(ctx context.Context) (int, error) {
	if err := m.ensureMovies(ctx); err != nil {
		return 0, err
	}
	m.peerMu.RLock()
	cli := m.moviesClient
	m.peerMu.RUnlock()
	if cli == nil {
		return 0, fmt.Errorf("movies client unavailable")
	}
	m.cfgMu.RLock()
	pageSize := m.ingestPageSize
	m.cfgMu.RUnlock()
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 100
	}
	count := 0
	seen := map[string]struct{}{}
	for page := int32(1); ; page++ {
		resp, err := cli.ListMovies(ctx, &mgmntv1.ListMoviesRequest{Page: page, PageSize: pageSize})
		if err != nil {
			return count, err
		}
		items := resp.GetMovies()
		if len(items) == 0 {
			break
		}
		for _, it := range items {
			seen[it.GetId()] = struct{}{}
			if _, err := m.upsertLibraryNode("movie", it.GetTitle(), movieExternalID(it.GetTmdbId()), map[string]string{
				"movie_id": it.GetId(),
				"tmdb_id":  strconv.FormatInt(int64(it.GetTmdbId()), 10),
				"year":     strconv.FormatInt(int64(it.GetYear()), 10),
				"imdb_id":  it.GetImdbId(),
			}); err == nil {
				count++
			}
		}
		if len(items) < int(pageSize) {
			break
		}
	}
	m.pruneLibraryNodes("movie", "movie_id", seen)
	return count, nil
}

func (m *Module) ingestTV(ctx context.Context) (int, error) {
	if err := m.ensureTV(ctx); err != nil {
		return 0, err
	}
	m.peerMu.RLock()
	cli := m.tvClient
	m.peerMu.RUnlock()
	if cli == nil {
		return 0, fmt.Errorf("tv client unavailable")
	}
	m.cfgMu.RLock()
	pageSize := m.ingestPageSize
	m.cfgMu.RUnlock()
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 100
	}
	count := 0
	seen := map[string]struct{}{}
	for page := int32(1); ; page++ {
		resp, err := cli.ListTVShows(ctx, &tvmgmtv1.ListTVShowsRequest{Page: page, PageSize: pageSize})
		if err != nil {
			return count, err
		}
		items := resp.GetSeries()
		if len(items) == 0 {
			break
		}
		for _, it := range items {
			seen[it.GetId()] = struct{}{}
			if _, err := m.upsertLibraryNode("series", it.GetName(), seriesExternalID(it.GetTmdbId()), map[string]string{
				"series_id": it.GetId(),
				"tmdb_id":   strconv.FormatInt(int64(it.GetTmdbId()), 10),
				"year":      strconv.FormatInt(int64(it.GetYear()), 10),
			}); err == nil {
				count++
			}
		}
		if len(items) < int(pageSize) {
			break
		}
	}
	m.pruneLibraryNodes("series", "series_id", seen)
	return count, nil
}

func (m *Module) ingestBooks(ctx context.Context) (int, error) {
	if err := m.ensureBooks(ctx); err != nil {
		return 0, err
	}
	m.peerMu.RLock()
	cli := m.booksClient
	m.peerMu.RUnlock()
	if cli == nil {
		return 0, fmt.Errorf("books client unavailable")
	}
	resp, err := cli.ListBooks(ctx, &booksv1.ListBooksRequest{})
	if err != nil {
		return 0, err
	}
	count := 0
	seen := map[string]struct{}{}
	for _, it := range resp.GetBooks() {
		seen[it.GetId()] = struct{}{}
		if _, err := m.upsertLibraryNode("book", it.GetTitle(), bookExternalID(it.GetIsbn()), map[string]string{
			"book_id":   it.GetId(),
			"author_id": it.GetAuthorId(),
			"isbn":      it.GetIsbn(),
			"year":      strconv.FormatInt(int64(it.GetYear()), 10),
		}); err == nil {
			count++
		}
	}
	m.pruneLibraryNodes("book", "book_id", seen)
	return count, nil
}

func (m *Module) ingestAlbums(ctx context.Context) (int, error) {
	if err := m.ensureMusic(ctx); err != nil {
		return 0, err
	}
	m.peerMu.RLock()
	cli := m.musicClient
	m.peerMu.RUnlock()
	if cli == nil {
		return 0, fmt.Errorf("music client unavailable")
	}
	m.cfgMu.RLock()
	pageSize := m.ingestPageSize
	m.cfgMu.RUnlock()
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 100
	}
	count := 0
	seen := map[string]struct{}{}
	for page := int32(1); ; page++ {
		resp, err := cli.ListArtists(ctx, &musicv1.ListArtistsRequest{Page: page, PageSize: pageSize})
		if err != nil {
			return count, err
		}
		artists := resp.GetArtists()
		if len(artists) == 0 {
			break
		}
		for _, artist := range artists {
			albums, err := cli.ListAlbums(ctx, &musicv1.ListAlbumsRequest{ArtistId: artist.GetId()})
			if err != nil {
				slog.Warn("media-graph: list albums", "artist", artist.GetId(), "error", err)
				continue
			}
			for _, it := range albums.GetAlbums() {
				seen[it.GetId()] = struct{}{}
				if _, err := m.upsertLibraryNode("album", it.GetTitle(), albumExternalID(it.GetMusicbrainzId()), map[string]string{
					"album_id":  it.GetId(),
					"artist_id": it.GetArtistId(),
					"mbid":      it.GetMusicbrainzId(),
					"year":      strconv.FormatInt(int64(it.GetYear()), 10),
				}); err == nil {
					count++
				}
			}
		}
		if len(artists) < int(pageSize) {
			break
		}
	}
	m.pruneLibraryNodes("album", "album_id", seen)
	return count, nil
}

func (m *Module) ingestComics(ctx context.Context) (int, error) {
	if err := m.ensureComics(ctx); err != nil {
		return 0, err
	}
	m.peerMu.RLock()
	cli := m.comicsClient
	m.peerMu.RUnlock()
	if cli == nil {
		return 0, fmt.Errorf("comics client unavailable")
	}
	resp, err := cli.ListSeries(ctx, &comicsv1.ListSeriesRequest{})
	if err != nil {
		return 0, err
	}
	count := 0
	seen := map[string]struct{}{}
	for _, it := range resp.GetSeries() {
		seen[it.GetId()] = struct{}{}
		if _, err := m.upsertLibraryNode("comic", it.GetTitle(), comicExternalID(it.GetComicvineId()), map[string]string{
			"series_id":    it.GetId(),
			"comicvine_id": it.GetComicvineId(),
			"publisher":    it.GetPublisher(),
		}); err == nil {
			count++
		}
	}
	m.pruneLibraryNodes("comic", "series_id", seen)
	return count, nil
}

func (m *Module) ingestAudiobooks(ctx context.Context) (int, error) {
	if err := m.ensureAudiobooks(ctx); err != nil {
		return 0, err
	}
	m.peerMu.RLock()
	cli := m.audiobooksClient
	m.peerMu.RUnlock()
	if cli == nil {
		return 0, fmt.Errorf("audiobooks client unavailable")
	}
	resp, err := cli.ListAudiobooks(ctx, &abv1.ListAudiobooksRequest{})
	if err != nil {
		return 0, err
	}
	count := 0
	seen := map[string]struct{}{}
	for _, it := range resp.GetAudiobooks() {
		seen[it.GetId()] = struct{}{}
		if _, err := m.upsertLibraryNode("audiobook", it.GetTitle(), audiobookExternalID(it.GetAsin()), map[string]string{
			"audiobook_id": it.GetId(),
			"author_id":    it.GetAuthorId(),
			"asin":         it.GetAsin(),
			"narrator":     it.GetNarrator(),
			"year":         strconv.FormatInt(int64(it.GetYear()), 10),
		}); err == nil {
			count++
		}
	}
	m.pruneLibraryNodes("audiobook", "audiobook_id", seen)
	return count, nil
}

func (m *Module) peerDialOptions() []grpc.DialOption {
	if m.grpcInsecure {
		return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
}

func (m *Module) dialPeer(ctx context.Context, capability string, setClient func(*grpc.ClientConn)) error {
	m.peerMu.RLock()
	mc := m.mc
	m.peerMu.RUnlock()
	if mc == nil {
		return fmt.Errorf("not connected to core")
	}
	addr, err := findCapabilityAddr(ctx, mc, capability)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(addr, m.peerDialOptions()...)
	if err != nil {
		return fmt.Errorf("dial %s: %w", capability, err)
	}
	m.peerMu.Lock()
	defer m.peerMu.Unlock()
	setClient(conn)
	return nil
}

func (m *Module) ensureMovies(ctx context.Context) error {
	m.peerMu.RLock()
	if m.moviesClient != nil {
		m.peerMu.RUnlock()
		return nil
	}
	m.peerMu.RUnlock()
	return m.dialPeer(ctx, "media.library.movies", func(conn *grpc.ClientConn) {
		if m.moviesClient == nil {
			m.moviesConn = conn
			m.moviesClient = mgmntv1.NewMovieManagementServiceClient(conn)
		} else {
			_ = conn.Close()
		}
	})
}

func (m *Module) ensureTV(ctx context.Context) error {
	m.peerMu.RLock()
	if m.tvClient != nil {
		m.peerMu.RUnlock()
		return nil
	}
	m.peerMu.RUnlock()
	return m.dialPeer(ctx, "media.library.tv", func(conn *grpc.ClientConn) {
		if m.tvClient == nil {
			m.tvConn = conn
			m.tvClient = tvmgmtv1.NewTvManagementServiceClient(conn)
		} else {
			_ = conn.Close()
		}
	})
}

func (m *Module) ensureBooks(ctx context.Context) error {
	m.peerMu.RLock()
	if m.booksClient != nil {
		m.peerMu.RUnlock()
		return nil
	}
	m.peerMu.RUnlock()
	return m.dialPeer(ctx, "media.books", func(conn *grpc.ClientConn) {
		if m.booksClient == nil {
			m.booksConn = conn
			m.booksClient = booksv1.NewBookManagementServiceClient(conn)
		} else {
			_ = conn.Close()
		}
	})
}

func (m *Module) ensureMusic(ctx context.Context) error {
	m.peerMu.RLock()
	if m.musicClient != nil {
		m.peerMu.RUnlock()
		return nil
	}
	m.peerMu.RUnlock()
	return m.dialPeer(ctx, "media.library.music", func(conn *grpc.ClientConn) {
		if m.musicClient == nil {
			m.musicConn = conn
			m.musicClient = musicv1.NewMusicManagementServiceClient(conn)
		} else {
			_ = conn.Close()
		}
	})
}

func (m *Module) ensureComics(ctx context.Context) error {
	m.peerMu.RLock()
	if m.comicsClient != nil {
		m.peerMu.RUnlock()
		return nil
	}
	m.peerMu.RUnlock()
	return m.dialPeer(ctx, "media.comics", func(conn *grpc.ClientConn) {
		if m.comicsClient == nil {
			m.comicsConn = conn
			m.comicsClient = comicsv1.NewComicManagementServiceClient(conn)
		} else {
			_ = conn.Close()
		}
	})
}

func (m *Module) ensureAudiobooks(ctx context.Context) error {
	m.peerMu.RLock()
	if m.audiobooksClient != nil {
		m.peerMu.RUnlock()
		return nil
	}
	m.peerMu.RUnlock()
	return m.dialPeer(ctx, "media.audiobooks", func(conn *grpc.ClientConn) {
		if m.audiobooksClient == nil {
			m.audiobooksConn = conn
			m.audiobooksClient = abv1.NewAudiobookManagementServiceClient(conn)
		} else {
			_ = conn.Close()
		}
	})
}

func findCapabilityAddr(ctx context.Context, mc *client.Client, capability string) (string, error) {
	modules, err := mc.Discovery.FindByCapability(ctx, capability)
	if err != nil {
		return "", fmt.Errorf("discover %s: %w", capability, err)
	}
	for _, mod := range modules {
		addr := dialAddrForModule(mod.Id, mod.HttpAddr)
		if addr != "" {
			return addr, nil
		}
	}
	return "", fmt.Errorf("no %s module found", capability)
}

func dialAddrForModule(moduleID, httpAddr string) string {
	if httpAddr == "" {
		return ""
	}
	host, port, err := net.SplitHostPort(httpAddr)
	if err != nil || port == "" {
		return httpAddr
	}
	if host != "" && host != "0.0.0.0" && host != "::" {
		return net.JoinHostPort(host, port)
	}
	if os.Getenv("MUXCORE_MESH_DIAL_LOCAL") == "true" {
		return net.JoinHostPort("127.0.0.1", port)
	}
	if moduleID != "" {
		return net.JoinHostPort(moduleID, port)
	}
	return httpAddr
}
