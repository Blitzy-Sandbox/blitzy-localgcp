// Package cloudrun - proxy.go implements the per-service HTTP reverse proxy
// with lazy-start Docker container semantics described by AAP §0.5.1.2
// Extension A ("Cloud Run actual execution").
//
// Lifecycle:
//
//	CreateService allocates a host port from the pool (8200-8299) and
//	constructs a serviceProxy that binds that port. The Docker image
//	reference is recorded but the container is NOT started yet.
//
//	On the FIRST inbound HTTP request, serveHTTP lazily invokes boot()
//	which pulls the image, creates and starts the container via the
//	injected orchestrator.ContainerRuntime, resolves the Docker-assigned
//	host port via HostPort(), and wires an httputil.ReverseProxy to
//	http://127.0.0.1:{dockerPort}. Subsequent requests short-circuit
//	through the cached reverse proxy with no additional container ops.
//
//	DeleteService calls Stop() which cancels the listener context,
//	gracefully shuts down the HTTP server, and (if a container was
//	booted) calls runtime.Stop() + runtime.Remove() to reclaim Docker
//	resources. The allocated host port is released to the pool by the
//	caller (cloudrun.Service.DeleteService) via store.ReleasePort.
//
// Design rules enforced here:
//
//   - Rule 1 (Docker boundary): this file ONLY interacts with Docker via
//     orchestrator.ContainerRuntime. No direct docker/docker SDK calls.
//   - Rule 4 (--no-docker unconditional): when the proxy's runtime field
//     is nil, serveHTTP short-circuits to 503 without ever calling any
//     container operation; this matches the semantics of a --no-docker
//     stub-mode listener that clients can still hit to get an error.
//   - 30s proxy response-header timeout per AAP §0.5.3.
package cloudrun

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/slokam-ai/localgcp/internal/orchestrator"
)

// proxyBootTimeout bounds how long a single first-request container boot
// (pull + create + start + port resolution + readiness wait) is allowed
// to take before we abort with a 502.
const proxyBootTimeout = 90 * time.Second

// proxyResponseHeaderTimeout bounds how long the proxy waits for the
// backend container to begin sending response headers. AAP §0.5.3
// specifies a 30-second proxy timeout.
const proxyResponseHeaderTimeout = 30 * time.Second

// readinessWait is the per-request window spent polling the Docker-
// assigned host port for connectivity after StartContainer succeeds.
// Containers frequently report "running" status before their in-process
// listener is accepting TCP connections; this short wait bridges that
// gap without blocking the caller for the full proxyBootTimeout.
const readinessWait = 10 * time.Second

// serviceProxy is the per-Cloud-Run-service reverse proxy with lazy
// Docker container boot. One serviceProxy is created for each
// CreateService call and stored in Service.proxies.
type serviceProxy struct {
	// Immutable configuration set at newServiceProxy time.
	name         string                        // full resource name (projects/…/services/…)
	image        string                        // Docker image reference
	internalPort string                        // internal container port, e.g. "8080/tcp"
	hostPort     int                           // reverse-proxy listener port, from the pool
	runtime      orchestrator.ContainerRuntime // nil in --no-docker stub mode
	logger       *log.Logger
	quiet        bool

	// Mutable state — first-request boot, listener lifecycle.
	once        sync.Once
	bootErr     error
	containerID string
	rp          *httputil.ReverseProxy

	srv    *http.Server
	ln     net.Listener
	cancel context.CancelFunc
}

// newServiceProxy constructs a proxy for a single Cloud Run service.
// If runtime is nil (e.g. --no-docker mode), the proxy still binds its
// listener port — this keeps the CreateService URI contract intact
// (non-empty URI pointing at a real listener) — but every request on
// that listener is answered with 503 Service Unavailable.
func newServiceProxy(name, image, internalPort string, hostPort int, runtime orchestrator.ContainerRuntime, logger *log.Logger, quiet bool) *serviceProxy {
	if internalPort == "" {
		internalPort = "8080/tcp"
	}
	if logger == nil {
		logger = log.Default()
	}
	return &serviceProxy{
		name:         name,
		image:        image,
		internalPort: internalPort,
		hostPort:     hostPort,
		runtime:      runtime,
		logger:       logger,
		quiet:        quiet,
	}
}

// Start binds the proxy's TCP listener on localhost:{hostPort} and
// begins serving HTTP requests in a background goroutine. Start does
// NOT block.
//
// If ctx is cancelled, the HTTP server is gracefully shut down via
// srv.Shutdown(). The container itself is NOT stopped on ctx cancel —
// use Stop() for full teardown (ensures StopContainer + RemoveContainer
// are invoked).
func (p *serviceProxy) Start(ctx context.Context) error {
	addr := fmt.Sprintf("127.0.0.1:%d", p.hostPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("cloud run proxy listen %s: %w", addr, err)
	}
	p.ln = ln

	proxyCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	p.srv = &http.Server{
		Handler:           http.HandlerFunc(p.serveHTTP),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Context cancellation triggers graceful shutdown of the proxy
	// listener (not the container).
	go func() {
		<-proxyCtx.Done()
		shutdownCtx, sc := context.WithTimeout(context.Background(), 3*time.Second)
		defer sc()
		_ = p.srv.Shutdown(shutdownCtx)
	}()

	go func() {
		err := p.srv.Serve(ln)
		if err != nil && err != http.ErrServerClosed && !p.quiet {
			p.logger.Printf("cloud run proxy %s: serve error: %v", p.name, err)
		}
	}()
	return nil
}

// Stop tears down the proxy: synchronously shuts down the HTTP server
// (releasing the TCP listener and thus the host port), stops the
// booted Docker container (if any), and removes it. Stop is idempotent
// and safe to call even if the proxy never received a request (and
// therefore never booted a container).
//
// Synchronous shutdown of the HTTP listener is important: the host
// port is reclaimed only when the listener is fully closed, and
// callers (cloudrun.Service.DeleteService) rely on this to release the
// port back to the 8200-8299 pool for immediate reuse.
//
// Race-hardening: there is an inherent race between Start's
// srv.Serve(ln) goroutine and Stop being called before Serve has
// registered the listener with the http.Server. In that race,
// srv.Shutdown() iterates its tracked-listener map (which is empty)
// and returns without closing the TCP socket. We therefore also close
// p.ln directly -- net.Listener.Close is idempotent and safe to call
// after Shutdown or before Serve has run.
func (p *serviceProxy) Stop(ctx context.Context) {
	// Signal the ctx.Done goroutine to exit (idempotent).
	if p.cancel != nil {
		p.cancel()
	}
	// Synchronously shut down the HTTP server. This drains in-flight
	// requests (bounded by the shutdown context timeout) and closes
	// any listeners that Serve() has already registered.
	if p.srv != nil {
		shutdownCtx, sc := context.WithTimeout(context.Background(), 3*time.Second)
		_ = p.srv.Shutdown(shutdownCtx)
		sc()
	}
	// Belt-and-braces: close the underlying TCP listener directly in
	// case Serve() had not yet registered it with the http.Server
	// when Shutdown ran. net.Listener.Close returns net.ErrClosed on
	// double-close, which we intentionally swallow.
	if p.ln != nil {
		_ = p.ln.Close()
	}
	if p.runtime == nil || p.containerID == "" {
		return
	}
	if err := p.runtime.Stop(ctx, p.containerID); err != nil && !p.quiet {
		p.logger.Printf("cloud run proxy %s: stop container: %v", p.name, err)
	}
	if err := p.runtime.Remove(ctx, p.containerID); err != nil && !p.quiet {
		p.logger.Printf("cloud run proxy %s: remove container: %v", p.name, err)
	}
}

// serveHTTP is the top-level HTTP handler for the proxy listener.
// It lazily boots the backing Docker container on the first request
// (once.Do) and then delegates to the cached httputil.ReverseProxy
// for every subsequent request.
func (p *serviceProxy) serveHTTP(w http.ResponseWriter, r *http.Request) {
	// --no-docker mode: no runtime configured. Respond with 503 so
	// clients see a real HTTP error rather than a TCP reset, but do
	// NOT attempt any container operations (Rule 4).
	if p.runtime == nil {
		http.Error(w,
			"localgcp: cloud run running in --no-docker mode; no container attached",
			http.StatusServiceUnavailable)
		return
	}

	// Lazy first-request boot. boot() is called exactly once per
	// serviceProxy instance. On failure the error is cached in
	// p.bootErr and every subsequent request returns 502.
	p.once.Do(func() {
		bootCtx, cancel := context.WithTimeout(r.Context(), proxyBootTimeout)
		defer cancel()
		p.bootErr = p.boot(bootCtx)
	})
	if p.bootErr != nil {
		http.Error(w,
			fmt.Sprintf("localgcp: cloud run container boot failed: %v", p.bootErr),
			http.StatusBadGateway)
		return
	}
	if p.rp == nil {
		http.Error(w,
			"localgcp: cloud run container not reachable",
			http.StatusBadGateway)
		return
	}
	p.rp.ServeHTTP(w, r)
}

// boot is the first-request Docker container lifecycle: pull image,
// create container, start container, resolve Docker-assigned host
// port, and wire up the reverse proxy. boot is invoked exactly once
// per serviceProxy via sync.Once; any error is cached in p.bootErr
// and returned by every subsequent serveHTTP call via a 502.
func (p *serviceProxy) boot(ctx context.Context) error {
	if !p.quiet {
		p.logger.Printf("cloud run proxy %s: booting container from %s", p.name, p.image)
	}

	if err := p.runtime.Pull(ctx, p.image); err != nil {
		return fmt.Errorf("pull %s: %w", p.image, err)
	}

	// Reuse an existing container of the same name if one is present
	// (possible after localgcp restart with --data-dir). Otherwise
	// Create a fresh one.
	cname := p.containerName()
	id, exists, findErr := p.runtime.FindExisting(ctx, cname)
	if findErr == nil && exists {
		p.containerID = id
	} else {
		newID, err := p.runtime.Create(ctx, orchestrator.ContainerConfig{
			Name:         cname,
			Image:        p.image,
			InternalPort: p.internalPort,
		})
		if err != nil {
			return fmt.Errorf("create container %s: %w", cname, err)
		}
		p.containerID = newID
	}

	if err := p.runtime.Start(ctx, p.containerID); err != nil {
		return fmt.Errorf("start container %s: %w", p.containerID, err)
	}

	// Resolve the Docker-assigned host port for the container's
	// InternalPort. DockerRuntime.HostPort returns a "127.0.0.1:49152"
	// style address.
	backend, err := p.runtime.HostPort(ctx, p.containerID, p.internalPort)
	if err != nil {
		return fmt.Errorf("resolve host port: %w", err)
	}

	target, err := url.Parse("http://" + backend)
	if err != nil {
		return fmt.Errorf("parse backend url: %w", err)
	}

	rp := httputil.NewSingleHostReverseProxy(target)
	rp.Transport = &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: proxyResponseHeaderTimeout,
	}
	rp.ErrorLog = p.logger
	rp.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		http.Error(w,
			fmt.Sprintf("localgcp: cloud run backend error: %v", err),
			http.StatusBadGateway)
	}
	p.rp = rp

	if !p.quiet {
		p.logger.Printf("cloud run proxy %s: container %s ready at %s",
			p.name, shortID(p.containerID), backend)
	}

	// Best-effort readiness wait. Many containers report "running"
	// before their listener is accepting connections; a short poll
	// smooths over the race without forcing the caller to retry.
	_ = waitForPort(ctx, backend, readinessWait)
	return nil
}

// containerName derives a Docker container name from the service's
// full resource name. Docker's naming rules forbid '/' and limit the
// character set, so we sanitize by keeping only [a-zA-Z0-9_-] and
// prefixing with "localgcp-cr-" for predictable cleanup by
// orchestrator.CleanupOrphans.
func (p *serviceProxy) containerName() string {
	var b strings.Builder
	b.Grow(len(p.name) + 16)
	b.WriteString("localgcp-cr-")
	for _, r := range p.name {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	// Trim trailing separators to avoid e.g. "localgcp-cr-foo-".
	out := b.String()
	for strings.HasSuffix(out, "-") && len(out) > len("localgcp-cr-") {
		out = out[:len(out)-1]
	}
	return out
}

// waitForPort polls the given TCP address until it accepts a
// connection or the timeout elapses. It never returns an error to the
// caller; it merely stalls for readiness. Always returns nil.
func waitForPort(ctx context.Context, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return nil
		}
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil
}

// shortID returns the first 12 chars of a Docker container ID for
// concise log output.
func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}
