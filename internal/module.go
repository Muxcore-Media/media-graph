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

	"google.golang.org/grpc"

	"github.com/Muxcore-Media/core/pkg/contracts"
	"github.com/Muxcore-Media/core/sdk/go/client"
	modulesdk "github.com/Muxcore-Media/core/sdk/go/module"
	mgmntv1 "github.com/Muxcore-Media/media-movies/proto/mgmntv1"
	tvmgmtv1 "github.com/Muxcore-Media/media-tvshows/proto/tvmgmtv1"
	mgv1 "github.com/Muxcore-Media/media-graph/proto/gen/muxcore/mediagraph/v1"
)

type Module struct {
	id, grpcAddr, httpAddr, defaultRel, dbPath string
	autoLink                                   bool
	ingestEnabled                              bool
	ingestInterval                             time.Duration
	ingestPageSize                             int32
	cfgMu                                      sync.RWMutex
	store                                      *Store
	grpcSrv                                    *grpc.Server
	lis                                        net.Listener
	httpSrv                                    *http.Server

	peerMu       sync.RWMutex
	mc           *client.Client
	moviesConn   *grpc.ClientConn
	moviesClient mgmntv1.MovieManagementServiceClient
	tvConn       *grpc.ClientConn
	tvClient     tvmgmtv1.TvManagementServiceClient
}

type Config struct {
	ID, DefaultRel, GRPCAddr, HTTPAddr, DBPath string
	AutoLink, IngestEnabled                    bool
	IngestInterval                             time.Duration
	IngestPageSize                             int32
}

func NewModule(cfg Config) *Module {
	if cfg.ID == "" {
		cfg.ID = "media-graph"
	}
	if cfg.GRPCAddr == "" {
		cfg.GRPCAddr = ":9730"
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":9731"
	}
	if cfg.DefaultRel == "" {
		cfg.DefaultRel = "related_to"
	}
	if cfg.DBPath == "" {
		cfg.DBPath = "./data/graph/graph.db"
	}
	cfg.AutoLink = true
	cfg.IngestEnabled = true
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
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.IngestPageSize = int32(n)
		}
	}
	if v := os.Getenv("MUXCORE_HTTP_ADDR"); v != "" {
		cfg.HTTPAddr = v
	}
	return &Module{
		id: cfg.ID, grpcAddr: cfg.GRPCAddr, httpAddr: cfg.HTTPAddr,
		defaultRel: cfg.DefaultRel, dbPath: cfg.DBPath, autoLink: cfg.AutoLink,
		ingestEnabled: cfg.IngestEnabled, ingestInterval: cfg.IngestInterval,
		ingestPageSize: cfg.IngestPageSize,
		store:          NewStore(),
	}
}

func (m *Module) Info() contracts.ModuleInfo {
	return contracts.ModuleInfo{
		ID: m.id, Name: "Media Graph", Version: "0.1.3",
		Roles:        []string{"media", "graph"},
		Description:  "Unified media graph with SQLite persistence, fixture ingest, related-title query, and admin JSON browser",
		Capabilities: []string{"media.graph", "graph", "settings"},
		HTTPAddr:     m.grpcAddr,
	}
}

func (m *Module) Init(ctx context.Context) error {
	if err := m.store.OpenDB(m.dbPath); err != nil {
		return err
	}
	slog.Info("media-graph initialized", "db", m.dbPath, "nodes", len(m.store.ListNodes()), "auto_link", m.autoLink)
	return nil
}

func (m *Module) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", m.grpcAddr)
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
	m.httpSrv = &http.Server{Addr: m.httpAddr, Handler: mux}
	go func() {
		slog.Info("health listening", "addr", m.httpAddr)
		if err := m.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("health serve", "error", err)
		}
	}()
	m.startIngest()
	return nil
}

func (m *Module) Stop(ctx context.Context) error {
	if m.grpcSrv != nil {
		m.grpcSrv.GracefulStop()
	}
	if m.httpSrv != nil {
		_ = m.httpSrv.Shutdown(ctx)
	}
	m.peerMu.Lock()
	if m.moviesConn != nil {
		_ = m.moviesConn.Close()
		m.moviesConn = nil
		m.moviesClient = nil
	}
	if m.tvConn != nil {
		_ = m.tvConn.Close()
		m.tvConn = nil
		m.tvClient = nil
	}
	if m.mc != nil {
		_ = m.mc.Close()
		m.mc = nil
	}
	m.peerMu.Unlock()
	_ = m.store.Close()
	return nil
}

func (m *Module) Health(ctx context.Context) error { return nil }

type graphServer struct {
	mgv1.UnimplementedMediaGraphServiceServer
	m *Module
}

func (s *graphServer) UpsertNode(_ context.Context, req *mgv1.UpsertNodeRequest) (*mgv1.UpsertNodeResponse, error) {
	n := req.GetNode()
	if n == nil {
		return nil, fmt.Errorf("node required")
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
		return nil, err
	}
	return &mgv1.UpsertNodeResponse{Node: toPBNode(out)}, nil
}

func (s *graphServer) GetNode(_ context.Context, req *mgv1.GetNodeRequest) (*mgv1.GetNodeResponse, error) {
	n, err := s.m.store.GetNode(req.GetId())
	if err != nil {
		return nil, err
	}
	return &mgv1.GetNodeResponse{Node: toPBNode(n)}, nil
}

func (s *graphServer) DeleteNode(_ context.Context, req *mgv1.DeleteNodeRequest) (*mgv1.DeleteNodeResponse, error) {
	if err := s.m.store.DeleteNode(req.GetId(), req.GetCascadeEdges()); err != nil {
		return nil, err
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
		return nil, err
	}
	return &mgv1.LinkResponse{Edge: toPBEdge(e)}, nil
}

func (s *graphServer) Unlink(_ context.Context, req *mgv1.UnlinkRequest) (*mgv1.UnlinkResponse, error) {
	if err := s.m.store.Unlink(req.GetEdgeId()); err != nil {
		return nil, err
	}
	return &mgv1.UnlinkResponse{Success: true}, nil
}

func (s *graphServer) Neighbors(_ context.Context, req *mgv1.NeighborsRequest) (*mgv1.NeighborsResponse, error) {
	n, edges, nodes, err := s.m.store.Neighbors(req.GetId(), req.GetRel(), int(req.GetDepth()))
	if err != nil {
		return nil, err
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
		return nil, err
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
		return nil, err
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
