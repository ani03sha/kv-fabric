package server

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/ani03sha/kv-fabric/consistency"
	pb "github.com/ani03sha/kv-fabric/proto"
	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/store"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This is the gRPC handler for the client-facing KV API.
//
// One KVServer runs per node. It is not aware of whether it is currently the leader: that check happens per-request
// via adapter.IsLeader(). This makes leader failover transparent: after election, the new leader's KVServer starts
// accepting writes without any wiring change.
//
// Redirect contract:
//   - Writes (Put, Delete) on a non-leader -> RedirectTo = leader's KV address.
//   - Strong reads on a non-leader         -> RedirectTo (ConfirmLeadership only works on the leader).
//   - RYW / Monotonic on a lagging follower -> RedirectTo (ErrNotCaughtUp from the router).
//   - The client is responsible for dialing the redirect address and retrying.
//     No internal server-to-server forwarding: that hides latency and makes traces ambiguous.
type KVServer struct {
	pb.UnimplementedKVFabricServer

	nodeID  string
	adapter *replication.RaftlyAdapter
	leader  *replication.LeaderReplicator
	engine  store.KVEngine
	tracker *replication.ReplicationTracker

	// Handles all four consistency modes on this node.
	// StrongReader and EventualReader are registered by NewRouter.
	// RYWReader and MonotonicReader are registered explicitly below.
	localRouter *consistency.Router

	// Maps nodeID → KV gRPC address for redirect responses.
	// Populated from --kv-peers flag: "node-2=host:9002,node-3=host:9003"
	kvPeers map[string]string

	logger *zap.Logger
}

func NewKVServer(
	nodeID string,
	adapter *replication.RaftlyAdapter,
	leader *replication.LeaderReplicator,
	engine store.KVEngine,
	tracker *replication.ReplicationTracker,
	kvPeers map[string]string,
	logger *zap.Logger,
) *KVServer {
	localRouter := consistency.NewRouter(nodeID, engine, adapter, tracker, logger)
	localRouter.RegisterRYW(consistency.NewRYWReader(nodeID, engine, adapter, logger))
	localRouter.RegisterMonotonic(consistency.NewMonotonicReader(nodeID, engine, adapter, logger))

	return &KVServer{
		nodeID:      nodeID,
		adapter:     adapter,
		leader:      leader,
		engine:      engine,
		tracker:     tracker,
		localRouter: localRouter,
		kvPeers:     kvPeers,
		logger:      logger,
	}
}

// Binds the gRPC listener and begins serving. Returns the *grpc.Server so the caller can call GracefulStop on shutdown.
func (s *KVServer) Start(addr string) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("kv server: listen %s: %w", addr, err)
	}
	srv := grpc.NewServer()
	pb.RegisterKVFabricServer(srv, s)
	go srv.Serve(lis)
	return srv, nil
}

func (s *KVServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if !s.adapter.IsLeader() {
		return &pb.PutResponse{RedirectTo: s.leaderKVAddr()}, nil
	}

	result, err := s.leader.Propose(ctx, replication.KVOperation{
		Type:      replication.OpPut,
		Key:       req.Key,
		Value:     req.Value,
		IfVersion: req.IfVersion,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "put %q: %v", req.Key, err)
	}

	// Issue a RYW session token so the client can do read-your-writes on any
	// follower without needing to know the leader's node ID.
	token, tokenErr := consistency.IssueToken(s.nodeID, result.Version)
	if tokenErr != nil {
		// Token failure is non-fatal: the write succeeded. Log and continue.
		s.logger.Warn("put: failed to issue session token",
			zap.String("key", req.Key),
			zap.Error(tokenErr))
		token = ""
	}

	return &pb.PutResponse{
		Version:      result.Version,
		SessionToken: token,
	}, nil
}

func (s *KVServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if !s.adapter.IsLeader() {
		return &pb.DeleteResponse{RedirectTo: s.leaderKVAddr()}, nil
	}

	_, err := s.leader.Propose(ctx, replication.KVOperation{
		Type: replication.OpDelete,
		Key:  req.Key,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete %q: %v", req.Key, err)
	}

	return &pb.DeleteResponse{}, nil
}

func (s *KVServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	// Strong reads run the ReadIndex protocol (ConfirmLeadership + waitForApplied).
	// ConfirmLeadership proposes a nil entry — only the leader can do this.
	// Redirect immediately rather than letting the router fail deep inside.
	if req.Consistency == pb.ConsistencyMode_STRONG && !s.adapter.IsLeader() {
		return &pb.GetResponse{RedirectTo: s.leaderKVAddr()}, nil
	}

	opts := store.GetOptions{
		Consistency:  store.ConsistencyMode(req.Consistency),
		SessionToken: req.SessionToken,
		MinVersion:   req.MinVersion,
	}

	result, err := s.localRouter.Get(ctx, req.Key, opts)
	if err != nil {
		var notCaughtUp *consistency.ErrNotCaughtUp
		if errors.As(err, &notCaughtUp) {
			// RYW or monotonic: this follower hasn't applied the required version yet.
			// Tell the client to retry at the leader — the leader is always caught up.
			return &pb.GetResponse{RedirectTo: s.leaderKVAddr()}, nil
		}
		return nil, status.Errorf(codes.Internal, "get %q: %v", req.Key, err)
	}

	return &pb.GetResponse{
		Value:    result.Value,
		Version:  result.Version,
		IsStale:  result.IsStale,
		LagMs:    result.LagMs,
		FromNode: result.FromNode,
	}, nil
}

// Scan goes directly to the engine. Range scans do not have a ReadIndex
// protocol equivalent in this implementation — they are always eventual.
// Documenting this explicitly prevents future callers from assuming
// strong semantics on a scan.
func (s *KVServer) Scan(_ context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	pairs, err := s.engine.Scan(req.Start, req.End, int(req.Limit))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "scan [%q, %q): %v", req.Start, req.End, err)
	}

	pbPairs := make([]*pb.KVPair, len(pairs))
	for i, p := range pairs {
		pbPairs[i] = &pb.KVPair{
			Key:     p.Key,
			Value:   p.Value,
			Version: p.Version,
		}
	}

	return &pb.ScanResponse{Pairs: pbPairs}, nil
}

func (s *KVServer) Status(_ context.Context, _ *pb.StatusRequest) (*pb.StatusResponse, error) {
	resp := &pb.StatusResponse{
		NodeId:       s.nodeID,
		IsLeader:     s.adapter.IsLeader(),
		LeaderId:     s.adapter.LeaderID(),
		CommitIndex:  s.adapter.CommitIndex(),
		AppliedIndex: s.adapter.AppliedIndex(),
	}

	for _, f := range s.tracker.Stats() {
		resp.Followers = append(resp.Followers, &pb.FollowerStatus{
			NodeId:     f.NodeID,
			MatchIndex: f.MatchIndex,
			LagEntries: f.LagEntries,
			LagMs:      f.LagMs,
		})
	}

	return resp, nil
}

// leaderKVAddr returns the KV gRPC address of the current Raft leader, or "" if the leader is unknown or has no
// registered KV address. The adapter's LeaderID() returns a node ID string ("node-1"); kvPeers maps that to a dial
// address ("host:9001").
func (s *KVServer) leaderKVAddr() string {
	leaderID := s.adapter.LeaderID()
	if leaderID == "" {
		return ""
	}
	return s.kvPeers[leaderID]
}
