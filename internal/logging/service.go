// Package logging implements the Cloud Logging emulator.
//
// This file hosts the gRPC Service that implements two API surfaces on a
// single port:
//
//   - google.logging.v2.LoggingServiceV2 — WriteLogEntries, ListLogEntries,
//     ListLogs, DeleteLog. Unchanged from the pre-extension behavior.
//   - google.logging.v2.ConfigServiceV2 — CreateSink, GetSink, UpdateSink,
//     DeleteSink, ListSinks. Added by AAP §0.5.1.2 Extension D. All other
//     ConfigServiceV2 RPCs (buckets, views, exclusions, settings, links,
//     CMEK) are inherited from UnimplementedConfigServiceV2Server which
//     returns the canonical gRPC Unimplemented error.
//
// Sink fan-out on WriteLogEntries: after the entries are persisted, a
// snapshot of matching sinks is taken and one goroutine is spawned per
// (entry, sink) pair to route the entry to the sink's destination. This is
// a fire-and-forget path (Rule 3): delivery failures are logged to stderr
// and never returned to the caller.
//
// Rule 7a: the New constructor accepts trailing variadic address arguments
// per AAP §0.5.1.2. Callers may invoke:
//
//	New(dataDir, quiet)                         // no loopback endpoints
//	New(dataDir, quiet, pubsubAddr)             // Pub/Sub fan-out only
//	New(dataDir, quiet, pubsubAddr, gcsAddr)    // both fan-out branches
//
// Empty strings (at either position) silently disable the corresponding
// fan-out branch with no error and no log. The legacy
// SetPubsubEndpoint / SetGcsEndpoint setters remain available for
// post-construction configuration and are semantically equivalent to
// passing the addresses at construction time.
package logging

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Service implements the Cloud Logging emulator.
//
// The service embeds both UnimplementedLoggingServiceV2Server and
// UnimplementedConfigServiceV2Server so all un-overridden RPCs on either
// surface return the canonical gRPC Unimplemented error.
type Service struct {
	loggingpb.UnimplementedLoggingServiceV2Server
	loggingpb.UnimplementedConfigServiceV2Server

	dataDir string
	quiet   bool
	logger  *log.Logger
	store   *Store

	// pubsubAddr and gcsAddr are the loopback endpoints used by the
	// WriteLogEntries sink fan-out path. Both default to "" which disables
	// the corresponding fan-out branch (Rule 7a). These fields are set via
	// SetPubsubEndpoint / SetGcsEndpoint and are expected to be fixed for
	// the lifetime of the process after Start is called.
	pubsubAddr string
	gcsAddr    string
}

// New creates a new Cloud Logging service.
//
// The signature (dataDir, quiet, addrs ...string) is the AAP §0.5.1.2
// variadic form. addrs[0], if present, is the loopback Pub/Sub endpoint
// used by `pubsub://...` and `pubsub.googleapis.com/...` sink
// destinations; addrs[1], if present, is the loopback GCS endpoint used
// by `storage.googleapis.com/...` sink destinations. Missing or empty
// values silently disable the corresponding fan-out branch (Rule 7a —
// no error, no log).
//
// Existing `New("", true)` call sites in service_test.go and integration
// tests continue to compile unchanged because variadic parameters accept
// zero trailing arguments. Callers that prefer post-construction wiring
// can still invoke the SetPubsubEndpoint / SetGcsEndpoint setters.
func New(dataDir string, quiet bool, addrs ...string) *Service {
	logger := log.New(os.Stderr, "[logging] ", log.LstdFlags)
	s := &Service{
		dataDir: dataDir,
		quiet:   quiet,
		logger:  logger,
		store:   NewStore(),
	}
	if len(addrs) >= 1 {
		s.pubsubAddr = addrs[0]
	}
	if len(addrs) >= 2 {
		s.gcsAddr = addrs[1]
	}
	return s
}

// SetPubsubEndpoint configures the loopback Pub/Sub endpoint used by the
// sink fan-out path for `pubsub.googleapis.com/...` destinations. An empty
// string disables Pub/Sub fan-out silently (Rule 7a — no error, no log).
//
// This method must be called before Start — the field is read by fan-out
// goroutines without synchronization.
func (s *Service) SetPubsubEndpoint(addr string) { s.pubsubAddr = addr }

// SetGcsEndpoint configures the loopback Cloud Storage endpoint used by
// the sink fan-out path for `storage.googleapis.com/...` destinations. An
// empty string disables GCS fan-out silently (Rule 7a — no error, no log).
//
// This method must be called before Start — the field is read by fan-out
// goroutines without synchronization.
func (s *Service) SetGcsEndpoint(addr string) { s.gcsAddr = addr }

func (s *Service) Name() string { return "Cloud Logging" }

func (s *Service) Start(ctx context.Context, addr string) error {
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(s.loggingInterceptor),
	)
	loggingpb.RegisterLoggingServiceV2Server(srv, s)
	loggingpb.RegisterConfigServiceV2Server(srv, s)
	reflection.Register(srv)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	if err := srv.Serve(ln); err != nil {
		return err
	}
	return nil
}

// --- LoggingServiceV2 (pre-existing surface, extended with sink fan-out) ---

// WriteLogEntries persists the log entries to the in-memory store and
// then fan-outs to all matching sinks in fire-and-forget goroutines. The
// fan-out never blocks the caller's response — per-sink delivery failures
// are written to stderr via the service's logger and never returned to
// the RPC caller (Rule 3).
func (s *Service) WriteLogEntries(_ context.Context, req *loggingpb.WriteLogEntriesRequest) (*loggingpb.WriteLogEntriesResponse, error) {
	entries := req.GetEntries()

	// Fill in defaults from the request-level fields.
	logName := req.GetLogName()
	resource := req.GetResource()
	labels := req.GetLabels()

	for _, entry := range entries {
		if entry.GetLogName() == "" && logName != "" {
			entry.LogName = logName
		}
		if entry.GetResource() == nil && resource != nil {
			entry.Resource = resource
		}
		if len(entry.GetLabels()) == 0 && len(labels) > 0 {
			entry.Labels = labels
		}
	}

	s.store.Write(entries)

	// Fire-and-forget sink fan-out (Rule 3). For each matching sink, spawn
	// one goroutine per entry. We snapshot the matching sinks under the
	// store's read lock; the snapshot is a value-type slice, so the
	// goroutines are free to iterate without holding the lock.
	//
	// When both pubsubAddr and gcsAddr are empty the entire fan-out is a
	// no-op and we skip the per-entry iteration to avoid wasted work.
	if s.pubsubAddr != "" || s.gcsAddr != "" {
		for _, entry := range entries {
			sinks := s.store.MatchingSinks(entry)
			for _, sink := range sinks {
				// Capture loop variables by value.
				sinkCopy := sink
				entryCopy := entry
				go deliverToSink(s.pubsubAddr, s.gcsAddr, sinkCopy, entryCopy)
			}
		}
	}

	return &loggingpb.WriteLogEntriesResponse{}, nil
}

func (s *Service) ListLogEntries(_ context.Context, req *loggingpb.ListLogEntriesRequest) (*loggingpb.ListLogEntriesResponse, error) {
	entries := s.store.List(req.GetResourceNames(), req.GetFilter(), int(req.GetPageSize()))
	return &loggingpb.ListLogEntriesResponse{Entries: entries}, nil
}

func (s *Service) ListLogs(_ context.Context, req *loggingpb.ListLogsRequest) (*loggingpb.ListLogsResponse, error) {
	logs := s.store.ListLogs(req.GetParent())
	return &loggingpb.ListLogsResponse{LogNames: logs}, nil
}

func (s *Service) DeleteLog(_ context.Context, req *loggingpb.DeleteLogRequest) (*emptypb.Empty, error) {
	s.store.DeleteLog(req.GetLogName())
	return &emptypb.Empty{}, nil
}

// --- ConfigServiceV2 (sinks — AAP §0.5.1.2 Extension D) ---

// CreateSink creates a sink under the given parent (e.g. "projects/p").
// The final sink name is "{parent}/sinks/{sink.Name}" where sink.Name is
// the short ID supplied by the client. An empty parent or missing name
// returns InvalidArgument. A pre-existing sink name returns AlreadyExists.
func (s *Service) CreateSink(_ context.Context, req *loggingpb.CreateSinkRequest) (*loggingpb.LogSink, error) {
	parent := req.GetParent()
	src := req.GetSink()
	if parent == "" {
		return nil, status.Error(codes.InvalidArgument, "parent is required")
	}
	if src == nil || src.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "sink.name is required")
	}
	if src.GetDestination() == "" {
		return nil, status.Error(codes.InvalidArgument, "sink.destination is required")
	}

	fullName := fmt.Sprintf("%s/sinks/%s", parent, src.GetName())
	internal := Sink{
		Name:           fullName,
		Destination:    src.GetDestination(),
		Filter:         src.GetFilter(),
		WriterIdentity: src.GetWriterIdentity(),
	}
	stored, err := s.store.CreateSink(internal)
	if err != nil {
		// Distinguish already-exists from other errors via the
		// package-level ErrSinkAlreadyExists sentinel (errors.Is, not
		// substring match — a future store message change must not
		// silently break the gRPC status code mapping).
		if errors.Is(err, ErrSinkAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "%s", err.Error())
		}
		return nil, status.Errorf(codes.Internal, "%s", err.Error())
	}
	return toLogSink(stored), nil
}

// GetSink returns the sink by its fully-qualified name.
func (s *Service) GetSink(_ context.Context, req *loggingpb.GetSinkRequest) (*loggingpb.LogSink, error) {
	name := req.GetSinkName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "sink_name is required")
	}
	stored, ok := s.store.GetSink(name)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "sink %q not found", name)
	}
	return toLogSink(stored), nil
}

// UpdateSink replaces the destination and filter of an existing sink. The
// SinkName field in the request identifies the sink to update; the Sink
// field carries the new values. CreateTime is preserved by the store.
func (s *Service) UpdateSink(_ context.Context, req *loggingpb.UpdateSinkRequest) (*loggingpb.LogSink, error) {
	name := req.GetSinkName()
	src := req.GetSink()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "sink_name is required")
	}
	if src == nil {
		return nil, status.Error(codes.InvalidArgument, "sink body is required")
	}
	internal := Sink{
		Name:           name,
		Destination:    src.GetDestination(),
		Filter:         src.GetFilter(),
		WriterIdentity: src.GetWriterIdentity(),
	}
	stored, err := s.store.UpdateSink(internal)
	if err != nil {
		// Distinguish not-found from other errors via the package-level
		// ErrSinkNotFound sentinel (errors.Is, not substring match).
		if errors.Is(err, ErrSinkNotFound) {
			return nil, status.Errorf(codes.NotFound, "%s", err.Error())
		}
		return nil, status.Errorf(codes.Internal, "%s", err.Error())
	}
	return toLogSink(stored), nil
}

// DeleteSink removes the sink by its fully-qualified name.
func (s *Service) DeleteSink(_ context.Context, req *loggingpb.DeleteSinkRequest) (*emptypb.Empty, error) {
	name := req.GetSinkName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "sink_name is required")
	}
	if !s.store.DeleteSink(name) {
		return nil, status.Errorf(codes.NotFound, "sink %q not found", name)
	}
	return &emptypb.Empty{}, nil
}

// ListSinks returns sinks under the given parent. Paging is not supported
// by the in-memory store — all sinks matching the parent prefix are
// returned in a single response.
func (s *Service) ListSinks(_ context.Context, req *loggingpb.ListSinksRequest) (*loggingpb.ListSinksResponse, error) {
	parent := req.GetParent()
	stored := s.store.ListSinks(parent)

	out := make([]*loggingpb.LogSink, 0, len(stored))
	for i := range stored {
		// Take address of slice element to avoid copying the sink twice.
		sink := stored[i]
		out = append(out, toLogSink(&sink))
	}
	return &loggingpb.ListSinksResponse{Sinks: out}, nil
}

// --- internal helpers ---

// toLogSink translates an internal Sink to the gRPC LogSink proto.
//
// CreateTime is always populated by Store.CreateSink at the moment the
// sink is inserted, so the stored value is trusted here without a
// fallback. UpdateTime is populated by CreateSink (to match CreateTime)
// and refreshed by UpdateSink, so it too is trusted as non-nil.
func toLogSink(s *Sink) *loggingpb.LogSink {
	if s == nil {
		return nil
	}
	return &loggingpb.LogSink{
		Name:           s.Name,
		Destination:    s.Destination,
		Filter:         s.Filter,
		WriterIdentity: s.WriterIdentity,
		CreateTime:     s.CreateTime,
		UpdateTime:     s.UpdateTime,
	}
}

func (s *Service) loggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	resp, err := handler(ctx, req)
	if !s.quiet {
		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}
		s.logger.Printf("%s %s", info.FullMethod, code)
	}
	return resp, err
}
