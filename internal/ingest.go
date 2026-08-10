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
	mgmntv1 "github.com/Muxcore-Media/media-movies/proto/mgmntv1"
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

func (m *Module) startIngest() {
	m.cfgMu.RLock()
	enabled := m.ingestEnabled
	m.cfgMu.RUnlock()
	if !enabled {
		slog.Info("media-graph: library ingest disabled (GRAPH_INGEST_ENABLED)")
		return
	}
	go m.dialCoreAndSubscribe()
	go m.ingestLoop()
}

func (m *Module) dialCoreAndSubscribe() {
	time.Sleep(8 * time.Second)
	meshAddr := os.Getenv("MUXCORE_GRPC_ADDR")
	if meshAddr == "" {
		meshAddr = "localhost:9090"
	}
	insecureMode := os.Getenv("MUXCORE_INSECURE_DISABLE_TLS") == "true" || os.Getenv("MUXCORE_GRPC_INSECURE") == "true"
	var opts []client.Option
	if insecureMode {
		opts = append(opts, client.WithInsecure())
	}
	c, err := client.Dial(meshAddr, opts...)
	if err != nil {
		slog.Error("media-graph: dial core", "error", err)
		return
	}
	m.peerMu.Lock()
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
		ch, cancel, err := c.Events.Subscribe(context.Background(), et)
		if err != nil {
			slog.Warn("media-graph: subscribe", "type", et, "error", err)
			continue
		}
		go m.handleIngestEvents(et, ch, cancel)
		slog.Info("media-graph: subscribed", "type", et)
	}
}

func (m *Module) handleIngestEvents(eventType string, ch <-chan *eventsv1.Event, cancel context.CancelFunc) {
	for evt := range ch {
		m.applyLibraryEvent(eventType, evt.GetPayload())
	}
	cancel()
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

func (m *Module) ingestLoop() {
	time.Sleep(12 * time.Second)
	m.ingestLibrariesOnce()
	for {
		m.cfgMu.RLock()
		enabled := m.ingestEnabled
		interval := m.ingestInterval
		m.cfgMu.RUnlock()
		if !enabled {
			time.Sleep(30 * time.Second)
			continue
		}
		if interval <= 0 {
			interval = 15 * time.Minute
		}
		timer := time.NewTimer(interval)
		<-timer.C
		m.ingestLibrariesOnce()
	}
}

func (m *Module) ingestLibrariesOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	movies, tv, errM, errT := 0, 0, error(nil), error(nil)
	movies, errM = m.ingestMovies(ctx)
	tv, errT = m.ingestTV(ctx)
	slog.Info("media-graph: library ingest pass",
		"movies", movies, "tv", tv, "movies_err", errM, "tv_err", errT)
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
			if _, err := m.upsertLibraryNode("movie", it.GetTitle(), movieExternalID(it.GetTmdbId()), map[string]string{
				"movie_id": it.GetId(),
				"tmdb_id":  strconv.FormatInt(int64(it.GetTmdbId()), 10),
				"year":     strconv.FormatInt(int64(it.GetYear()), 10),
				"imdb_id":  it.GetImdbId(),
			}); err == nil {
				count++
			}
		}
		if int32(len(items)) < pageSize {
			break
		}
	}
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
			if _, err := m.upsertLibraryNode("series", it.GetName(), seriesExternalID(it.GetTmdbId()), map[string]string{
				"series_id": it.GetId(),
				"tmdb_id":   strconv.FormatInt(int64(it.GetTmdbId()), 10),
				"year":      strconv.FormatInt(int64(it.GetYear()), 10),
			}); err == nil {
				count++
			}
		}
		if int32(len(items)) < pageSize {
			break
		}
	}
	return count, nil
}

func (m *Module) ensureMovies(ctx context.Context) error {
	m.peerMu.RLock()
	if m.moviesClient != nil {
		m.peerMu.RUnlock()
		return nil
	}
	mc := m.mc
	m.peerMu.RUnlock()
	if mc == nil {
		return fmt.Errorf("not connected to core")
	}
	addr, err := findCapabilityAddr(ctx, mc, "media.library.movies")
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial movies: %w", err)
	}
	m.peerMu.Lock()
	if m.moviesClient == nil {
		m.moviesConn = conn
		m.moviesClient = mgmntv1.NewMovieManagementServiceClient(conn)
	} else {
		_ = conn.Close()
	}
	m.peerMu.Unlock()
	return nil
}

func (m *Module) ensureTV(ctx context.Context) error {
	m.peerMu.RLock()
	if m.tvClient != nil {
		m.peerMu.RUnlock()
		return nil
	}
	mc := m.mc
	m.peerMu.RUnlock()
	if mc == nil {
		return fmt.Errorf("not connected to core")
	}
	addr, err := findCapabilityAddr(ctx, mc, "media.library.tv")
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial tv: %w", err)
	}
	m.peerMu.Lock()
	if m.tvClient == nil {
		m.tvConn = conn
		m.tvClient = tvmgmtv1.NewTvManagementServiceClient(conn)
	} else {
		_ = conn.Close()
	}
	m.peerMu.Unlock()
	return nil
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
