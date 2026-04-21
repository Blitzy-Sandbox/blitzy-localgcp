// Package cloudrun implements the Google Cloud Run (Services API) emulator.
//
// This file implements the five in-scope gRPC RPCs -- CreateService,
// GetService, ListServices, UpdateService, DeleteService -- atop the
// in-memory Store (store.go) and per-service reverse proxies (proxy.go).
//
// Lazy-start Docker semantics
//
//	Per AAP §0.1.1 Extension A, CreateService does NOT start a
//	container. It allocates a host port from the 8200-8299 pool and
//	binds a reverse-proxy listener on that port. The URI returned to
//	the client is http://localhost:{hostPort}.
//
//	The first HTTP request to that URI lazily starts the container
//	via the injected orchestrator.ContainerRuntime; subsequent
//	requests are forwarded directly. DeleteService cascades
//	stopContainer + removeContainer through the runtime and releases
//	the host port back to the pool.
//
// --no-docker mode (Rule 4)
//
//	If SetNoDocker(true) is called (wired from cfg.NoDocker in main.go)
//	OR if no ContainerRuntime is configured (SetRuntime was never
//	called), CreateService still allocates a host port and still
//	returns a non-empty URI of the form http://localhost:{hostPort},
//	but the proxy runs in "stub mode" -- every request is answered
//	with 503 Service Unavailable and NO container operations occur.
//	This is verified by the nodocker_test.go unit test using a
//	failing-mock runtime.
//
// Out-of-scope RPCs (Rule 6)
//
//	GetIamPolicy, SetIamPolicy, TestIamPermissions return the canonical
//	codes.Unimplemented error with message
//	"localgcp: {FullMethodName} not yet supported".
package cloudrun

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/slokam-ai/localgcp/internal/orchestrator"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// defaultInternalPort is the in-container port the Cloud Run emulator
// assumes the user's application listens on unless the service template
// specifies a container port explicitly via Container.Ports.
const defaultInternalPort = "8080/tcp"

// Service implements the Cloud Run emulator (Services API).
//
// The runtime and noDocker fields are configured after construction
// via SetRuntime and SetNoDocker (see AAP §0.4.1.4 — the New(dataDir,
// quiet) signature is preserved for Rule 7). main.go invokes the
// setters during service registration.
type Service struct {
	runpb.UnimplementedServicesServer

	dataDir string
	quiet   bool
	logger  *log.Logger
	store   *Store

	// runtime is the Docker abstraction used for lazy container
	// start. Nil means "no runtime available" -- treated the same
	// as noDocker=true.
	runtime orchestrator.ContainerRuntime
	// noDocker short-circuits all container operations when true.
	noDocker bool

	// proxies holds the per-service serviceProxy instances keyed by
	// the service's full resource name (projects/.../services/...).
	proxiesMu sync.Mutex
	proxies   map[string]*serviceProxy

	// serveCtx is the parent context that proxy goroutines inherit
	// from. It is set by Start(ctx) and cancelled on server shutdown.
	// serveCtx is nil until Start is invoked.
	serveCtx context.Context
}

// New creates a new Cloud Run service with an empty store and the
// default port pool (8200-8299). The runtime and noDocker fields are
// zero-valued; main.go should call SetRuntime and SetNoDocker before
// Start() is invoked to wire up the Docker bridge.
//
// Signature preserved for Rule 7 test compatibility.
func New(dataDir string, quiet bool) *Service {
	logger := log.New(os.Stderr, "[cloudrun] ", log.LstdFlags)
	return &Service{
		dataDir: dataDir,
		quiet:   quiet,
		logger:  logger,
		store:   NewStore(),
		proxies: make(map[string]*serviceProxy),
	}
}

// SetRuntime wires an orchestrator.ContainerRuntime onto the Service.
// Must be invoked BEFORE Start() if real container execution is desired.
// If left unset (or set to nil), the service behaves identically to
// --no-docker mode: CreateService still allocates a port and returns a
// non-empty URI, but every HTTP request to that URI answers with 503.
func (s *Service) SetRuntime(r orchestrator.ContainerRuntime) {
	s.runtime = r
}

// SetNoDocker toggles --no-docker stub mode. When true, no container
// operations are attempted regardless of whether a runtime is set
// (Rule 4: unconditional honoring of --no-docker).
func (s *Service) SetNoDocker(b bool) {
	s.noDocker = b
}

// Name implements server.Service. Returns "Cloud Run".
func (s *Service) Name() string { return "Cloud Run" }

// Start binds the gRPC server on addr and serves the five in-scope
// RPCs plus the canonical-unimplemented handlers for out-of-scope IAM
// RPCs. The serve context is stashed on the Service so per-service
// proxy listeners spawned by CreateService inherit cancellation from
// the parent.
//
// Lazy runtime auto-initialization: when --no-docker is NOT in effect
// and no runtime was injected via SetRuntime, a fresh DockerRuntime
// is constructed here. This ensures that instantiating cloudrun.New
// stand-alone (e.g., in unit tests or non-standard wiring paths)
// still yields a functional service without the caller having to
// remember to call SetRuntime. Rule 4 is preserved: when noDocker is
// true no runtime is created and all container operations are
// unconditionally skipped downstream.
func (s *Service) Start(ctx context.Context, addr string) error {
	if !s.noDocker && s.runtime == nil {
		s.runtime = orchestrator.NewDockerRuntime(s.logger)
	}

	s.serveCtx = ctx

	srv := grpc.NewServer(
		grpc.UnaryInterceptor(s.loggingInterceptor),
	)
	runpb.RegisterServicesServer(srv, s)
	reflection.Register(srv)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	go func() {
		<-ctx.Done()
		// Gracefully stop the gRPC server.
		srv.GracefulStop()
		// Tear down every per-service proxy (cancels listeners and
		// stops/removes booted containers).
		s.stopAllProxies()
	}()

	if err := srv.Serve(ln); err != nil {
		return err
	}
	return nil
}

// stopAllProxies drains the proxies map and calls Stop on each. Uses
// a background context because the serve context may already be
// cancelled by the time we reach here.
func (s *Service) stopAllProxies() {
	s.proxiesMu.Lock()
	proxies := make([]*serviceProxy, 0, len(s.proxies))
	for _, p := range s.proxies {
		proxies = append(proxies, p)
	}
	s.proxies = make(map[string]*serviceProxy)
	s.proxiesMu.Unlock()

	for _, p := range proxies {
		p.Stop(context.Background())
	}
}

// dockerEnabled reports whether the Service should attempt real
// container operations. Both a non-nil runtime AND !noDocker are
// required.
func (s *Service) dockerEnabled() bool {
	return s.runtime != nil && !s.noDocker
}

// proxyRuntime returns the ContainerRuntime to pass into the per-
// service proxy. It returns nil (stub mode) whenever --no-docker is
// in effect OR the runtime has not been configured.
func (s *Service) proxyRuntime() orchestrator.ContainerRuntime {
	if !s.dockerEnabled() {
		return nil
	}
	return s.runtime
}

// CreateService inserts a new Cloud Run service, allocates a host
// port from the pool, and wires up a reverse-proxy listener on that
// port. The URI returned is always http://localhost:{hostPort} --
// AAP Extension A requirement (§0.1.1).
func (s *Service) CreateService(ctx context.Context, req *runpb.CreateServiceRequest) (*longrunning.Operation, error) {
	parent := req.GetParent()
	serviceID := req.GetServiceId()
	name := parent + "/services/" + serviceID

	// Insert into the store first. If the service already exists
	// this short-circuits before any port allocation side-effects.
	svc, err := s.store.Create(name, req.GetService())
	if err != nil {
		return nil, status.Errorf(codes.AlreadyExists, "Service %s already exists", name)
	}

	// Allocate a host port from the 8200-8299 pool (Rule 8). If the
	// pool is exhausted we roll back the store insert and surface
	// the canonical ResourceExhausted error.
	hostPort, perr := s.store.AllocatePort()
	if perr != nil {
		s.store.Delete(name)
		return nil, perr
	}

	uri := fmt.Sprintf("http://localhost:%d", hostPort)
	s.store.SetURI(name, uri)

	// Extract the image reference from the template. Cloud Run
	// requires exactly one container in the template; we tolerate
	// a missing image by letting the proxy surface a 502 when boot
	// is attempted.
	image := extractImage(svc)
	internalPort := extractInternalPort(svc)
	if internalPort == "" {
		internalPort = defaultInternalPort
	}
	s.store.SetRef(name, &ContainerRef{
		HostPort:     hostPort,
		Image:        image,
		InternalPort: internalPort,
	})

	// Build and start the per-service reverse proxy. In --no-docker
	// mode proxyRuntime() returns nil, which causes the proxy to
	// serve 503 on every request without ever touching Docker.
	// The store is threaded through so boot() can persist the Docker
	// container ID into ContainerRef.ContainerID once the container
	// is successfully started (closes the orphan-container window).
	proxy := newServiceProxy(name, image, internalPort, hostPort, s.proxyRuntime(), s.store, s.logger, s.quiet)
	proxyCtx := s.serveCtx
	if proxyCtx == nil {
		proxyCtx = context.Background()
	}
	if err := proxy.Start(proxyCtx); err != nil {
		// Listener bind failed -- roll back everything.
		s.store.Delete(name)
		s.store.ReleasePort(hostPort)
		return nil, status.Errorf(codes.Internal, "cloud run: start proxy listener: %v", err)
	}

	s.proxiesMu.Lock()
	s.proxies[name] = proxy
	s.proxiesMu.Unlock()

	// Re-read the stored svc so its Uri is populated.
	if current, ok := s.store.Get(name); ok {
		svc = current
	}
	if !s.quiet {
		mode := "docker"
		if !s.dockerEnabled() {
			mode = "stub"
		}
		s.logger.Printf("CreateService %s -> %s (image=%s mode=%s)", name, uri, image, mode)
	}
	return completedOp(name+"/operations/create", svc)
}

// GetService returns the named service from the store, or NotFound.
func (s *Service) GetService(_ context.Context, req *runpb.GetServiceRequest) (*runpb.Service, error) {
	svc, ok := s.store.Get(req.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Service %s not found", req.GetName())
	}
	return svc, nil
}

// ListServices returns all services under the given parent.
func (s *Service) ListServices(_ context.Context, req *runpb.ListServicesRequest) (*runpb.ListServicesResponse, error) {
	services := s.store.List(req.GetParent())
	return &runpb.ListServicesResponse{Services: services}, nil
}

// UpdateService merges the provided service back onto the stored one.
// The reverse proxy listener and URI are preserved -- Cloud Run
// semantics treat an update as a revision rollout, which we simulate
// by keeping the existing container attached.
func (s *Service) UpdateService(_ context.Context, req *runpb.UpdateServiceRequest) (*longrunning.Operation, error) {
	svc := req.GetService()
	name := svc.GetName()

	updated, err := s.store.Update(name, svc)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "Service %s not found", name)
	}

	return completedOp(name+"/operations/update", updated)
}

// DeleteService removes the service, tears down its proxy and
// container, and releases the host port. Cascade failures (container
// stop/remove) are logged but do not prevent deletion.
func (s *Service) DeleteService(_ context.Context, req *runpb.DeleteServiceRequest) (*longrunning.Operation, error) {
	name := req.GetName()

	ref, hadRef := s.store.GetRef(name)
	if !s.store.Delete(name) {
		return nil, status.Errorf(codes.NotFound, "Service %s not found", name)
	}

	// Tear down the proxy (which in turn stops+removes the booted
	// container, if any). Uses background context because the
	// RPC's own ctx may be cancelled before the Docker API calls
	// complete.
	s.proxiesMu.Lock()
	proxy, ok := s.proxies[name]
	if ok {
		delete(s.proxies, name)
	}
	s.proxiesMu.Unlock()
	if ok {
		proxy.Stop(context.Background())
	}

	// Release the allocated port back to the pool.
	if hadRef && ref != nil {
		s.store.ReleasePort(ref.HostPort)
	}

	return completedOp(name+"/operations/delete", &runpb.Service{Name: name})
}

// --- Out-of-scope RPCs (Rule 6) ---------------------------------------------

// GetIamPolicy returns the canonical Unimplemented error (Rule 6).
func (s *Service) GetIamPolicy(_ context.Context, _ *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return nil, unimplemented("/google.cloud.run.v2.Services/GetIamPolicy")
}

// SetIamPolicy returns the canonical Unimplemented error (Rule 6).
func (s *Service) SetIamPolicy(_ context.Context, _ *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return nil, unimplemented("/google.cloud.run.v2.Services/SetIamPolicy")
}

// TestIamPermissions returns the canonical Unimplemented error (Rule 6).
func (s *Service) TestIamPermissions(_ context.Context, _ *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	return nil, unimplemented("/google.cloud.run.v2.Services/TestIamPermissions")
}

// unimplemented is the canonical error helper required by AAP Rule 6.
// Single-sourced so the format string never drifts.
func unimplemented(fullMethod string) error {
	return status.Errorf(codes.Unimplemented, "localgcp: %s not yet supported", fullMethod)
}

// --- Helpers ----------------------------------------------------------------

// extractImage returns the first container's image from the service
// template, or "" if no containers are defined.
func extractImage(svc *runpb.Service) string {
	tpl := svc.GetTemplate()
	if tpl == nil {
		return ""
	}
	containers := tpl.GetContainers()
	if len(containers) == 0 {
		return ""
	}
	return containers[0].GetImage()
}

// extractInternalPort returns the first container's first declared
// port as a "N/tcp" string, or "" if no ports are declared. The
// defaultInternalPort constant is substituted by CreateService if
// this returns "".
func extractInternalPort(svc *runpb.Service) string {
	tpl := svc.GetTemplate()
	if tpl == nil {
		return ""
	}
	containers := tpl.GetContainers()
	if len(containers) == 0 {
		return ""
	}
	ports := containers[0].GetPorts()
	if len(ports) == 0 {
		return ""
	}
	return fmt.Sprintf("%d/tcp", ports[0].GetContainerPort())
}

// completedOp wraps a result in an immediately-completed long-running
// operation.
func completedOp(opName string, result *runpb.Service) (*longrunning.Operation, error) {
	any, err := anypb.New(result)
	if err != nil {
		return nil, fmt.Errorf("marshal operation result: %w", err)
	}
	return &longrunning.Operation{
		Name:   opName,
		Done:   true,
		Result: &longrunning.Operation_Response{Response: any},
	}, nil
}

// loggingInterceptor is the standard unary interceptor used by every
// native gRPC service in localgcp. Gated by the --quiet flag.
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
