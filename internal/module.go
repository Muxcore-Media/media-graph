package internal

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	manifest "github.com/Muxcore-Media/media-graph"

	"google.golang.org/grpc"

	"github.com/Muxcore-Media/core/pkg/contracts"
	"github.com/Muxcore-Media/core/sdk/go/client"
	modulesdk "github.com/Muxcore-Media/core/sdk/go/module"
	abv1 "github.com/Muxcore-Media/media-audiobooks/proto/gen/muxcore/audiobooks/v1"
	booksv1 "github.com/Muxcore-Media/media-books/proto/gen/muxcore/books/v1"
	comicsv1 "github.com/Muxcore-Media/media-comics/proto/gen/muxcore/comics/v1"
	mgv1 "github.com/Muxcore-Media/media-graph/proto/gen/muxcore/mediagraph/v1"
	mgmntv1 "github.com/Muxcore-Media/media-movies/proto/mgmntv1"
	musicv1 "github.com/Muxcore-Media/media-music/proto/gen/muxcore/music/v1"
	tvmgmtv1 "github.com/Muxcore-Media/media-tvshows/proto/tvmgmtv1"
)

type Module struct {
	lis              net.Listener
	tvClient         tvmgmtv1.TvManagementServiceClient
	moviesClient     mgmntv1.MovieManagementServiceClient
	booksClient      booksv1.BookManagementServiceClient
	musicClient      musicv1.MusicManagementServiceClient
	comicsClient     comicsv1.ComicManagementServiceClient
	audiobooksClient abv1.AudiobookManagementServiceClient
	mc               *client.Client
	store            *Store
	tvConn           *grpc.ClientConn
	moviesConn       *grpc.ClientConn
	booksConn        *grpc.ClientConn
	musicConn        *grpc.ClientConn
	comicsConn       *grpc.ClientConn
	audiobooksConn   *grpc.ClientConn
	httpSrv          *http.Server
	grpcSrv          *grpc.Server
	defaultRel       string
	dbPath           string
	fixturePath      string
	adminToken       string
	id               string
	httpAddr         string
	grpcAddr         string
	ingestInterval   time.Duration
	cfgMu            sync.RWMutex
	peerMu           sync.RWMutex
	ingestMu         sync.Mutex
	eventMu          sync.Mutex
	ingestWG         sync.WaitGroup
	ingestCancel     context.CancelFunc
	eventCancels     []context.CancelFunc
	ingestPageSize   int32
	ingestEnabled    bool
	autoLink         bool
	grpcInsecure     bool
}

type Config struct {
	ID             string
	DefaultRel     string
	GRPCAddr       string
	HTTPAddr       string
	DBPath         string
	FixturePath    string
	AdminToken     string
	IngestInterval time.Duration
	IngestPageSize int32
	AutoLink       bool
	IngestEnabled  bool
	GRPCInsecure   bool
}

func NewModule(cfg Config) *Module {
	if cfg.ID == "" {
		cfg.ID = "media-graph"
	}
	if cfg.GRPCAddr == "" {
		if v := os.Getenv("GRAPH_GRPC_ADDR"); v != "" {
			cfg.GRPCAddr = v
		} else {
			cfg.GRPCAddr = ":9730"
		}
	}
	if cfg.HTTPAddr == "" {
		if v := os.Getenv("MUXCORE_HTTP_ADDR"); v != "" {
			cfg.HTTPAddr = v
		} else {
			cfg.HTTPAddr = "127.0.0.1:9731"
		}
	}
	if cfg.DefaultRel == "" {
		cfg.DefaultRel = "related_to"
	}
	if cfg.DBPath == "" {
		cfg.DBPath = "./data/graph/graph.db"
	}
	if cfg.FixturePath == "" {
		cfg.FixturePath = os.Getenv("GRAPH_FIXTURE_PATH")
	}
	if cfg.AdminToken == "" {
		cfg.AdminToken = moduleTokenFromEnv()
	}
	if cfg.IngestInterval <= 0 {
		cfg.IngestInterval = 15 * time.Minute
	}
	if cfg.IngestPageSize <= 0 {
		cfg.IngestPageSize = 100
	}
	if v := os.Getenv("GRAPH_DEFAULT_REL"); v != "" {
		cfg.DefaultRel = v
	}
	if v := os.Getenv("GRAPH_DB_PATH"); v != "" {
		cfg.DBPath = v
	}
	if v := os.Getenv("GRAPH_FIXTURE_PATH"); v != "" {
		cfg.FixturePath = v
	}
	if v := os.Getenv("GRAPH_AUTO_LINK"); v != "" {
		cfg.AutoLink = v == "1" || v == "true" || v == "TRUE"
	}
	if v := os.Getenv("GRAPH_INGEST_ENABLED"); v != "" {
		cfg.IngestEnabled = v == "1" || v == "true" || v == "TRUE"
	}
	if v := os.Getenv("GRAPH_INGEST_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.IngestInterval = d
		}
	}
	if v := os.Getenv("GRAPH_INGEST_PAGE_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			cfg.IngestPageSize = int32(n) //nolint:gosec // bounded to 1000 above
		}
	}
	cfg.GRPCInsecure = os.Getenv("MUXCORE_INSECURE_DISABLE_TLS") == "true" || os.Getenv("MUXCORE_GRPC_INSECURE") == "true"
	return &Module{
		id: cfg.ID, grpcAddr: cfg.GRPCAddr, httpAddr: cfg.HTTPAddr,
		defaultRel: cfg.DefaultRel, dbPath: cfg.DBPath, fixturePath: cfg.FixturePath,
		adminToken: cfg.AdminToken, autoLink: cfg.AutoLink,
		ingestEnabled: cfg.IngestEnabled, ingestInterval: cfg.IngestInterval,
		ingestPageSize: cfg.IngestPageSize, grpcInsecure: cfg.GRPCInsecure,
		store: NewStore(),
	}
}

func (m *Module) Info() contracts.ModuleInfo {
	return contracts.ModuleInfo{
		ID: m.id, Name: "Media Graph", Version: modulesdk.ManifestVersion(manifest.ManifestJSON),
		Roles:        []string{"media", "graph"},
		Description:  "Unified media graph with SQLite persistence, fixture ingest, related-title query, and admin JSON browser",
		Capabilities: []string{"media.graph", "graph", "settings"},
		HTTPAddr:     m.grpcAddr,
	}
}

func (m *Module) Init(ctx context.Context) error {
	if err := m.store.OpenDB(ctx, m.dbPath); err != nil {
		return err
	}
	slog.Info("media-graph initialized", "db", m.dbPath, "nodes", len(m.store.ListNodes()), "auto_link", m.autoLink)
	return nil
}

func (m *Module) Start(ctx context.Context) error {
	if m.fixturePath != "" {
		res, err := m.IngestLibraryFixtures(m.fixturePath)
		if err != nil {
			slog.Warn("media-graph: fixture ingest", "path", m.fixturePath, "error", err)
		} else {
			slog.Info("media-graph: fixture ingest", "path", m.fixturePath, "movies", res.Movies, "series", res.Series, "edges", res.Edges)
		}
	}
	lc := net.ListenConfig{}
	lis, err := lc.Listen(ctx, "tcp", m.grpcAddr)
	if err != nil {
		return fmt.Errorf("listen gRPC %s: %w", m.grpcAddr, err)
	}
	m.lis = lis
	m.grpcSrv = grpc.NewServer()
	mgv1.RegisterMediaGraphServiceServer(m.grpcSrv, &graphServer{m: m})
	modulesdk.RegisterSettings(m.grpcSrv, m.id, m)
	go func() {
		slog.Info("media-graph gRPC listening", "addr", m.grpcAddr)
		if err := m.grpcSrv.Serve(lis); err != nil {
			slog.Error("gRPC serve", "error", err)
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	m.registerAdminRoutes(mux)
	m.httpSrv = &http.Server{
		Addr:         m.httpAddr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	go func() {
		slog.Info("health listening", "addr", m.httpAddr)
		if err := m.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("health serve", "error", err)
		}
	}()
	m.startIngest(ctx)
	return nil
}

func (m *Module) Stop(ctx context.Context) error {
	m.stopIngest()
	m.eventMu.Lock()
	for _, cancel := range m.eventCancels {
		cancel()
	}
	m.eventCancels = nil
	m.eventMu.Unlock()
	if m.grpcSrv != nil {
		m.grpcSrv.GracefulStop()
	}
	if m.httpSrv != nil {
		_ = m.httpSrv.Shutdown(ctx)
	}
	m.peerMu.Lock()
	for _, conn := range []*grpc.ClientConn{
		m.moviesConn, m.tvConn, m.booksConn, m.musicConn, m.comicsConn, m.audiobooksConn,
	} {
		if conn != nil {
			_ = conn.Close()
		}
	}
	m.moviesConn, m.tvConn, m.booksConn, m.musicConn, m.comicsConn, m.audiobooksConn = nil, nil, nil, nil, nil, nil
	m.moviesClient, m.tvClient, m.booksClient, m.musicClient, m.comicsClient, m.audiobooksClient = nil, nil, nil, nil, nil, nil
	if m.mc != nil {
		_ = m.mc.Close()
		m.mc = nil
	}
	m.peerMu.Unlock()
	_ = m.store.Close()
	return nil
}

func (m *Module) Health(ctx context.Context) error {
	if err := m.store.PingDB(ctx); err != nil {
		return fmt.Errorf("sqlite ping: %w", err)
	}
	return nil
}

type graphServer struct {
	mgv1.UnimplementedMediaGraphServiceServer
	m *Module
}

func (s *graphServer) UpsertNode(ctx context.Context, req *mgv1.UpsertNodeRequest) (*mgv1.UpsertNodeResponse, error) {
	n := req.GetNode()
	if n == nil {
		return nil, toGRPCError(fmt.Errorf("node required"))
	}
	node := Node{
		ID: n.GetId(), Kind: n.GetKind(), Title: n.GetTitle(),
		ExternalID: n.GetExternalId(), Attrs: n.GetAttrs(),
	}
	if node.ID == "" && node.ExternalID != "" {
		if existing := s.m.store.FindByExternalID(node.ExternalID); existing != nil {
			node.ID = existing.ID
		}
	}
	s.m.cfgMu.RLock()
	auto := s.m.autoLink
	s.m.cfgMu.RUnlock()
	out, _, err := s.m.store.UpsertNodeWithAutoLink(node, auto)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &mgv1.UpsertNodeResponse{Node: toPBNode(out)}, nil
}

func (s *graphServer) GetNode(_ context.Context, req *mgv1.GetNodeRequest) (*mgv1.GetNodeResponse, error) {
	if req.GetId() == "" && req.GetExternalId() != "" {
		n := s.m.store.FindByExternalID(req.GetExternalId())
		if n == nil {
			return nil, toGRPCError(fmt.Errorf("node with external_id %q not found", req.GetExternalId()))
		}
		return &mgv1.GetNodeResponse{Node: toPBNode(n)}, nil
	}
	if req.GetId() == "" {
		return nil, toGRPCError(fmt.Errorf("id or external_id required"))
	}
	n, err := s.m.store.GetNode(req.GetId())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &mgv1.GetNodeResponse{Node: toPBNode(n)}, nil
}

func (s *graphServer) DeleteNode(_ context.Context, req *mgv1.DeleteNodeRequest) (*mgv1.DeleteNodeResponse, error) {
	if err := s.m.store.DeleteNode(req.GetId(), req.GetCascadeEdges()); err != nil {
		return nil, toGRPCError(err)
	}
	return &mgv1.DeleteNodeResponse{Success: true}, nil
}

func (s *graphServer) SearchNodes(_ context.Context, req *mgv1.SearchNodesRequest) (*mgv1.SearchNodesResponse, error) {
	items := s.m.store.Search(req.GetQuery(), req.GetKind(), int(req.GetLimit()))
	out := make([]*mgv1.Node, 0, len(items))
	for _, n := range items {
		out = append(out, toPBNode(n))
	}
	return &mgv1.SearchNodesResponse{Nodes: out}, nil
}

func (s *graphServer) Link(_ context.Context, req *mgv1.LinkRequest) (*mgv1.LinkResponse, error) {
	rel := req.GetRel()
	if rel == "" {
		s.m.cfgMu.RLock()
		rel = s.m.defaultRel
		s.m.cfgMu.RUnlock()
	}
	e, err := s.m.store.Link(req.GetFromId(), req.GetToId(), rel, req.GetWeight())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &mgv1.LinkResponse{Edge: toPBEdge(e)}, nil
}

func (s *graphServer) Unlink(_ context.Context, req *mgv1.UnlinkRequest) (*mgv1.UnlinkResponse, error) {
	if err := s.m.store.Unlink(req.GetEdgeId()); err != nil {
		return nil, toGRPCError(err)
	}
	return &mgv1.UnlinkResponse{Success: true}, nil
}

func (s *graphServer) Neighbors(_ context.Context, req *mgv1.NeighborsRequest) (*mgv1.NeighborsResponse, error) {
	n, edges, nodes, err := s.m.store.Neighbors(req.GetId(), req.GetRel(), int(req.GetDepth()))
	if err != nil {
		return nil, toGRPCError(err)
	}
	pe := make([]*mgv1.Edge, 0, len(edges))
	for i := range edges {
		pe = append(pe, toPBEdge(&edges[i]))
	}
	pn := make([]*mgv1.Node, 0, len(nodes))
	for i := range nodes {
		pn = append(pn, toPBNode(&nodes[i]))
	}
	return &mgv1.NeighborsResponse{Node: toPBNode(n), Edges: pe, Nodes: pn}, nil
}

func (s *graphServer) Path(_ context.Context, req *mgv1.PathRequest) (*mgv1.PathResponse, error) {
	ids, edges, found, err := s.m.store.Path(req.GetFromId(), req.GetToId(), int(req.GetMaxDepth()))
	if err != nil {
		return nil, toGRPCError(err)
	}
	pe := make([]*mgv1.Edge, 0, len(edges))
	for i := range edges {
		pe = append(pe, toPBEdge(&edges[i]))
	}
	return &mgv1.PathResponse{NodeIds: ids, Edges: pe, Found: found}, nil
}

func (s *graphServer) GetRelatedTitles(_ context.Context, req *mgv1.GetRelatedTitlesRequest) (*mgv1.GetRelatedTitlesResponse, error) {
	n, related, err := s.m.store.RelatedTitles(
		req.GetId(), req.GetExternalId(), req.GetRel(),
		int(req.GetDepth()), int(req.GetLimit()),
	)
	if err != nil {
		return nil, toGRPCError(err)
	}
	out := make([]*mgv1.RelatedTitle, 0, len(related))
	for i := range related {
		rt := related[i]
		out = append(out, &mgv1.RelatedTitle{
			Node: toPBNode(&rt.Node), Rel: rt.Rel, Weight: rt.Weight,
		})
	}
	return &mgv1.GetRelatedTitlesResponse{Node: toPBNode(n), Related: out}, nil
}

func toPBNode(n *Node) *mgv1.Node {
	return &mgv1.Node{
		Id: n.ID, Kind: n.Kind, Title: n.Title, ExternalId: n.ExternalID, Attrs: n.Attrs,
	}
}

func toPBEdge(e *Edge) *mgv1.Edge {
	return &mgv1.Edge{Id: e.ID, FromId: e.FromID, ToId: e.ToID, Rel: e.Rel, Weight: e.Weight}
}
