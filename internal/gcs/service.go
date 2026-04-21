package gcs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Service implements the Cloud Storage emulator.
type Service struct {
	dataDir string
	quiet   bool
	logger  *log.Logger
	store   *Store

	// resumable uploads in progress: upload ID -> pending upload state
	resumableMu sync.Mutex
	resumables  map[string]*resumableUpload

	// pubsubAddr is the loopback Pub/Sub endpoint used for notification
	// fan-out. Empty string disables fan-out entirely (AAP Rule 7a).
	// Set via SetPubsubEndpoint after construction.
	pubsubAddr string
}

type resumableUpload struct {
	Bucket      string
	Name        string
	ContentType string
	Data        bytes.Buffer
}

// New creates a new GCS service.
//
// The signature is intentionally preserved as (dataDir, quiet) to honor the
// AAP Rule 7a "zero call-site changes in test files" criterion. The
// cross-service Pub/Sub loopback address is configured post-construction
// via SetPubsubEndpoint, matching the setter pattern used by the Cloud Run
// service package.
func New(dataDir string, quiet bool) *Service {
	logger := log.New(os.Stderr, "[gcs] ", log.LstdFlags)
	return &Service{
		dataDir:    dataDir,
		quiet:      quiet,
		logger:     logger,
		store:      NewStore(dataDir),
		resumables: make(map[string]*resumableUpload),
	}
}

// SetPubsubEndpoint configures the loopback Pub/Sub endpoint used by the
// notification-config fan-out path. An empty string disables fan-out
// (silently skipped — no error, no log) per AAP Rule 7a.
//
// This method must be called before Start — the value is read from fan-out
// goroutines without synchronization. Calling it after Start is racy.
func (s *Service) SetPubsubEndpoint(addr string) {
	s.pubsubAddr = addr
}

func (s *Service) Name() string { return "Cloud Storage" }

func (s *Service) Start(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	s.registerRoutes(mux)

	srv := &http.Server{
		Addr:    addr,
		Handler: s.loggingMiddleware(mux),
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	if err := srv.Serve(ln); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// registerRoutes sets up all GCS JSON API routes.
//
// GCS JSON API path structure:
//
//	/storage/v1/b                          — list/create buckets
//	/storage/v1/b/{bucket}                 — get/delete bucket
//	/storage/v1/b/{bucket}/o               — list objects
//	/storage/v1/b/{bucket}/o/{object...}   — get/delete object (object can contain /)
//	/upload/storage/v1/b/{bucket}/o        — upload objects
//
// Object names can contain slashes, so we can't use simple path params.
// We route by prefix and parse manually.
func (s *Service) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/storage/v1/b", s.route)
	mux.HandleFunc("/storage/v1/b/", s.route)
	mux.HandleFunc("/upload/storage/v1/b/", s.handleUpload)
	mux.HandleFunc("/download/storage/v1/b/", s.handleDownload)
	mux.HandleFunc("/_localgcp/sign", s.handleSign)
	mux.HandleFunc("/", s.handleDefault)
}

// handleDownload serves object content via the /download/ path prefix.
// The Go storage client uses this path for NewReader: GET /download/storage/v1/b/{bucket}/o/{object}?alt=media
func (s *Service) handleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "methodNotAllowed", "Method not allowed")
		return
	}

	// Path: /download/storage/v1/b/{bucket}/o/{object...}
	rest := strings.TrimPrefix(r.URL.Path, "/download/storage/v1/b/")
	oIdx := strings.Index(rest, "/o/")
	if oIdx < 0 {
		writeNotFound(w, "Invalid download path")
		return
	}
	bucket := rest[:oIdx]
	objectName := rest[oIdx+3:]

	meta, content, ok := s.store.GetObject(bucket, objectName)
	if !ok {
		writeNotFound(w, fmt.Sprintf("No such object: %s/%s", bucket, objectName))
		return
	}

	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Content-Length", meta.Size)
	w.Header().Set("X-Goog-Hash", fmt.Sprintf("md5=%s", meta.Md5Hash))
	w.WriteHeader(http.StatusOK)
	w.Write(content)
}

func (s *Service) route(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	// /storage/v1/b — bucket list or create
	if path == "/storage/v1/b" || path == "/storage/v1/b/" {
		switch r.Method {
		case http.MethodGet:
			s.handleListBuckets(w, r)
		case http.MethodPost:
			s.handleCreateBucket(w, r)
		default:
			writeError(w, http.StatusMethodNotAllowed, "methodNotAllowed", "Method not allowed")
		}
		return
	}

	// Strip prefix to get: {bucket} or {bucket}/o or {bucket}/o/{object...}
	rest := strings.TrimPrefix(path, "/storage/v1/b/")

	// Check for copy: {bucket}/o/{src}/copyTo/b/{dstBucket}/o/{dstObj}
	if strings.Contains(rest, "/copyTo/b/") {
		if r.Method == http.MethodPost {
			s.handleCopyObject(w, r, rest)
		} else {
			writeError(w, http.StatusMethodNotAllowed, "methodNotAllowed", "Method not allowed")
		}
		return
	}

	// Check for /notificationConfigs (notification-config CRUD, AAP Extension B).
	// Path shapes after trimming the /storage/v1/b/ prefix:
	//   {bucket}/notificationConfigs         — list / create
	//   {bucket}/notificationConfigs/{id}    — get / delete
	if ncIdx := strings.Index(rest, "/notificationConfigs"); ncIdx > 0 {
		bucket := rest[:ncIdx]
		tail := rest[ncIdx+len("/notificationConfigs"):]
		switch {
		case tail == "" || tail == "/":
			// collection-level: list (GET) or create (PUT/POST)
			switch r.Method {
			case http.MethodGet:
				s.handleNotificationList(w, r, bucket)
			case http.MethodPut, http.MethodPost:
				s.handleNotificationCreate(w, r, bucket)
			default:
				writeError(w, http.StatusMethodNotAllowed, "methodNotAllowed", "Method not allowed")
			}
		default:
			// item-level: path is "/{id}" — trim leading slash
			id := strings.TrimPrefix(tail, "/")
			// reject nested sub-paths like /{id}/extra
			if strings.Contains(id, "/") {
				writeNotFound(w, fmt.Sprintf("Path not found: %s", r.URL.Path))
				return
			}
			switch r.Method {
			case http.MethodGet:
				s.handleNotificationGet(w, r, bucket, id)
			case http.MethodDelete:
				s.handleNotificationDelete(w, r, bucket, id)
			default:
				writeError(w, http.StatusMethodNotAllowed, "methodNotAllowed", "Method not allowed")
			}
		}
		return
	}

	// Check for /o/ (object operations)
	oIdx := strings.Index(rest, "/o/")
	if oIdx >= 0 {
		bucket := rest[:oIdx]
		objectName := rest[oIdx+3:] // everything after /o/
		switch r.Method {
		case http.MethodGet:
			s.handleGetObject(w, r, bucket, objectName)
		case http.MethodDelete:
			s.handleDeleteObject(w, r, bucket, objectName)
		default:
			writeError(w, http.StatusMethodNotAllowed, "methodNotAllowed", "Method not allowed")
		}
		return
	}

	// Check for /o (object list, no trailing object name)
	if strings.HasSuffix(rest, "/o") {
		bucket := strings.TrimSuffix(rest, "/o")
		if r.Method == http.MethodGet {
			s.handleListObjects(w, r, bucket)
		} else {
			writeError(w, http.StatusMethodNotAllowed, "methodNotAllowed", "Method not allowed")
		}
		return
	}

	// Otherwise it's a bucket operation: {bucket}
	bucket := rest
	switch r.Method {
	case http.MethodGet:
		s.handleGetBucket(w, r, bucket)
	case http.MethodDelete:
		s.handleDeleteBucket(w, r, bucket)
	default:
		writeError(w, http.StatusMethodNotAllowed, "methodNotAllowed", "Method not allowed")
	}
}

// --- Bucket handlers ---

func (s *Service) handleCreateBucket(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Name == "" {
		writeBadRequest(w, "Bucket name is required")
		return
	}

	project := r.URL.Query().Get("project")
	if project == "" {
		project = "localgcp"
	}

	b, err := s.store.CreateBucket(body.Name, project)
	if err != nil {
		if strings.Contains(err.Error(), "conflict") {
			writeConflict(w, fmt.Sprintf("You already own this bucket: %s", body.Name))
			return
		}
		writeError(w, http.StatusInternalServerError, "internalError", err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(b)
}

func (s *Service) handleGetBucket(w http.ResponseWriter, r *http.Request, name string) {
	b, ok := s.store.GetBucket(name)
	if !ok {
		writeNotFound(w, fmt.Sprintf("The specified bucket does not exist: %s", name))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(b)
}

func (s *Service) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	buckets := s.store.ListBuckets()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(BucketList{Kind: "storage#buckets", Items: buckets})
}

func (s *Service) handleDeleteBucket(w http.ResponseWriter, r *http.Request, name string) {
	err := s.store.DeleteBucket(name)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeNotFound(w, fmt.Sprintf("The specified bucket does not exist: %s", name))
		} else if strings.Contains(err.Error(), "not empty") {
			writeConflict(w, "The bucket you tried to delete is not empty.")
		} else {
			writeError(w, http.StatusInternalServerError, "internalError", err.Error())
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- Notification configuration handlers (AAP Extension B) ---
//
// These handlers expose the per-bucket NotificationConfig CRUD surface at
//   /storage/v1/b/{bucket}/notificationConfigs[/{id}]
// matching the GCS JSON API. Creation accepts a subset of the full
// NotificationConfig proto (topic is required; event_types, custom
// attributes, object_name_prefix, and payload_format are optional).
// The server assigns the id.

func (s *Service) handleNotificationCreate(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, ok := s.store.GetBucket(bucket); !ok {
		writeNotFound(w, fmt.Sprintf("The specified bucket does not exist: %s", bucket))
		return
	}

	var cfg NotificationConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		writeBadRequest(w, "Invalid JSON body")
		return
	}
	if cfg.TopicName == "" {
		writeBadRequest(w, "topic is required (projects/{project}/topics/{topic})")
		return
	}

	created, err := s.store.CreateNotification(bucket, cfg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internalError", err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(created)
}

func (s *Service) handleNotificationGet(w http.ResponseWriter, r *http.Request, bucket, id string) {
	if _, ok := s.store.GetBucket(bucket); !ok {
		writeNotFound(w, fmt.Sprintf("The specified bucket does not exist: %s", bucket))
		return
	}
	cfg, ok := s.store.GetNotification(bucket, id)
	if !ok {
		writeNotFound(w, fmt.Sprintf("No such notification: %s/%s", bucket, id))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cfg)
}

func (s *Service) handleNotificationList(w http.ResponseWriter, r *http.Request, bucket string) {
	items, bucketExists := s.store.ListNotifications(bucket)
	if !bucketExists {
		writeNotFound(w, fmt.Sprintf("The specified bucket does not exist: %s", bucket))
		return
	}
	if items == nil {
		items = []NotificationConfig{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(NotificationList{
		Kind:  "storage#notifications",
		Items: items,
	})
}

func (s *Service) handleNotificationDelete(w http.ResponseWriter, r *http.Request, bucket, id string) {
	if err := s.store.DeleteNotification(bucket, id); err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeNotFound(w, fmt.Sprintf("No such notification: %s/%s", bucket, id))
			return
		}
		writeError(w, http.StatusInternalServerError, "internalError", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- Object event fan-out (AAP Extension B) ---
//
// fanoutObjectEvent delivers a notification to every matching config on the
// bucket. Delivery happens on a per-config goroutine so the request handler
// never blocks on Pub/Sub (Rule 3). Delivery failures are logged to stderr
// and never surfaced to the caller.
//
// Matching rules (following real GCS semantics):
//   - empty EventTypes means "match all event types"
//   - empty ObjectNamePrefix means "match all object names"
//
// The payload is the full Object metadata JSON, matching the JSON_API_V1
// payload format. Attributes include the GCS-canonical {eventType,
// bucketId} pair required by the AAP.
func (s *Service) fanoutObjectEvent(bucket string, obj Object, eventType string) {
	if s.pubsubAddr == "" {
		// Fan-out is disabled (Rule 7a silent skip).
		return
	}

	configs := s.store.NotificationsForBucket(bucket)
	if len(configs) == 0 {
		return
	}

	for i := range configs {
		cfg := configs[i]
		if !cfg.matchesEvent(eventType) {
			continue
		}
		if !cfg.matchesObject(obj.Name) {
			continue
		}
		// Fire-and-forget — one goroutine per matching config.
		go s.deliverNotification(cfg, obj, eventType, bucket)
	}
}

// deliverNotification publishes a single object event to the Pub/Sub topic
// referenced by cfg. Invoked from a goroutine; never returns a value.
func (s *Service) deliverNotification(cfg NotificationConfig, obj Object, eventType, bucketID string) {
	// PayloadFormat=NONE means the body should be empty (GCS convention).
	var payload []byte
	if cfg.PayloadFormat != "NONE" {
		var err error
		payload, err = json.Marshal(obj)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[gcs] notification: marshal object %s/%s: %v\n", bucketID, obj.Name, err)
			return
		}
	}

	// Attributes: AAP-required eventType and bucketId, plus any
	// user-defined custom_attributes on the config.
	attrs := map[string]string{
		"eventType":          eventType,
		"bucketId":           bucketID,
		"objectId":           obj.Name,
		"objectGeneration":   obj.Etag,
		"payloadFormat":      cfg.PayloadFormat,
		"notificationConfig": fmt.Sprintf("projects/_/buckets/%s/notificationConfigs/%s", bucketID, cfg.ID),
	}
	for k, v := range cfg.CustomAttributes {
		// Don't let user-defined attributes override the canonical ones.
		if _, exists := attrs[k]; !exists {
			attrs[k] = v
		}
	}

	if err := publishToPubsub(s.pubsubAddr, cfg.TopicName, payload, attrs); err != nil {
		fmt.Fprintf(os.Stderr, "[gcs] notification: deliver %s → %s: %v\n", obj.Name, cfg.TopicName, err)
	}
}

// matchesEvent reports whether the config should fan-out for the given
// event type. An empty EventTypes slice means "match all events".
func (n *NotificationConfig) matchesEvent(eventType string) bool {
	if len(n.EventTypes) == 0 {
		return true
	}
	for _, t := range n.EventTypes {
		if t == eventType {
			return true
		}
	}
	return false
}

// matchesObject reports whether the config should fan-out for the given
// object name. An empty ObjectNamePrefix means "match all objects".
func (n *NotificationConfig) matchesObject(objectName string) bool {
	if n.ObjectNamePrefix == "" {
		return true
	}
	return strings.HasPrefix(objectName, n.ObjectNamePrefix)
}

// --- Object handlers ---

func (s *Service) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, name string) {
	meta, content, ok := s.store.GetObject(bucket, name)
	if !ok {
		writeNotFound(w, fmt.Sprintf("No such object: %s/%s", bucket, name))
		return
	}

	// alt=media means download the content, otherwise return metadata.
	if r.URL.Query().Get("alt") == "media" {
		w.Header().Set("Content-Type", meta.ContentType)
		w.Header().Set("Content-Length", meta.Size)
		w.Header().Set("X-Goog-Hash", fmt.Sprintf("md5=%s", meta.Md5Hash))
		w.WriteHeader(http.StatusOK)
		w.Write(content)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(meta)
}

func (s *Service) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, name string) {
	// Capture the object metadata BEFORE deletion so the OBJECT_DELETE
	// notification payload carries the object's last-known state. The
	// store's DeleteObject only returns an error (not the removed object),
	// so we snapshot via GetObject first. If the object does not exist,
	// the DeleteObject call below will produce the canonical "not found"
	// error and we skip the fan-out. GetObject returns
	// (meta *Object, content []byte, ok bool) — we only need the meta.
	meta, _, existed := s.store.GetObject(bucket, name)

	if err := s.store.DeleteObject(bucket, name); err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeNotFound(w, fmt.Sprintf("No such object: %s/%s", bucket, name))
		} else {
			writeError(w, http.StatusInternalServerError, "internalError", err.Error())
		}
		return
	}

	// Fire-and-forget OBJECT_DELETE notification fan-out (Rule 3).
	// meta is a defensive copy returned by GetObject, so it may be safely
	// handed to the goroutine after this function returns.
	if existed && meta != nil {
		s.fanoutObjectEvent(bucket, *meta, "OBJECT_DELETE")
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleListObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, ok := s.store.GetBucket(bucket); !ok {
		writeNotFound(w, fmt.Sprintf("The specified bucket does not exist: %s", bucket))
		return
	}

	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	items, prefixes := s.store.ListObjects(bucket, prefix, delimiter, 0)

	if items == nil {
		items = []Object{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ObjectList{
		Kind:     "storage#objects",
		Items:    items,
		Prefixes: prefixes,
	})
}

func (s *Service) handleCopyObject(w http.ResponseWriter, r *http.Request, rest string) {
	// Format: {srcBucket}/o/{srcObject}/copyTo/b/{dstBucket}/o/{dstObject}
	parts := strings.SplitN(rest, "/o/", 2)
	if len(parts) != 2 {
		writeBadRequest(w, "Invalid copy path")
		return
	}
	srcBucket := parts[0]

	copyIdx := strings.Index(parts[1], "/copyTo/b/")
	if copyIdx < 0 {
		writeBadRequest(w, "Invalid copy path")
		return
	}
	srcObject := parts[1][:copyIdx]

	dstPart := parts[1][copyIdx+len("/copyTo/b/"):]
	dstParts := strings.SplitN(dstPart, "/o/", 2)
	if len(dstParts) != 2 {
		writeBadRequest(w, "Invalid copy destination path")
		return
	}
	dstBucket := dstParts[0]
	dstObject := dstParts[1]

	obj, err := s.store.CopyObject(srcBucket, srcObject, dstBucket, dstObject)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeNotFound(w, err.Error())
		} else {
			writeError(w, http.StatusInternalServerError, "internalError", err.Error())
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(obj)
}

// --- Upload handlers ---

func (s *Service) handleUpload(w http.ResponseWriter, r *http.Request) {
	// Path: /upload/storage/v1/b/{bucket}/o
	path := strings.TrimPrefix(r.URL.Path, "/upload/storage/v1/b/")
	bucket := strings.TrimSuffix(path, "/o")

	if _, ok := s.store.GetBucket(bucket); !ok {
		writeNotFound(w, fmt.Sprintf("The specified bucket does not exist: %s", bucket))
		return
	}

	uploadType := r.URL.Query().Get("uploadType")

	switch uploadType {
	case "media":
		s.handleSimpleUpload(w, r, bucket)
	case "multipart":
		s.handleMultipartUpload(w, r, bucket)
	case "resumable":
		if r.Method == http.MethodPost {
			s.handleResumableInit(w, r, bucket)
		} else if r.Method == http.MethodPut {
			s.handleResumableChunk(w, r, bucket)
		} else {
			writeError(w, http.StatusMethodNotAllowed, "methodNotAllowed", "Method not allowed")
		}
	default:
		writeBadRequest(w, "uploadType must be media, multipart, or resumable")
	}
}

func (s *Service) handleSimpleUpload(w http.ResponseWriter, r *http.Request, bucket string) {
	name := r.URL.Query().Get("name")
	if name == "" {
		writeBadRequest(w, "Object name is required (query param 'name')")
		return
	}

	content, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internalError", "Failed to read body")
		return
	}

	contentType := r.Header.Get("Content-Type")
	obj, err := s.store.PutObject(bucket, name, contentType, content)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internalError", err.Error())
		return
	}

	// Fire-and-forget notification fan-out (Rule 3). A snapshot of the
	// Object value is copied onto the goroutine stack so the handler may
	// return immediately.
	s.fanoutObjectEvent(bucket, *obj, "OBJECT_FINALIZE")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(obj)
}

func (s *Service) handleMultipartUpload(w http.ResponseWriter, r *http.Request, bucket string) {
	contentType := r.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.HasPrefix(mediaType, "multipart/") {
		writeBadRequest(w, "Content-Type must be multipart/related")
		return
	}

	reader := multipart.NewReader(r.Body, params["boundary"])

	// First part: JSON metadata.
	metaPart, err := reader.NextPart()
	if err != nil {
		writeBadRequest(w, "Failed to read metadata part")
		return
	}

	var meta struct {
		Name        string `json:"name"`
		ContentType string `json:"contentType"`
	}
	if err := json.NewDecoder(metaPart).Decode(&meta); err != nil {
		writeBadRequest(w, "Invalid JSON metadata")
		return
	}
	metaPart.Close()

	if meta.Name == "" {
		writeBadRequest(w, "Object name is required in metadata")
		return
	}

	// Second part: file content.
	dataPart, err := reader.NextPart()
	if err != nil {
		writeBadRequest(w, "Failed to read data part")
		return
	}
	content, err := io.ReadAll(dataPart)
	dataPart.Close()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internalError", "Failed to read data")
		return
	}

	obj, err := s.store.PutObject(bucket, meta.Name, meta.ContentType, content)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internalError", err.Error())
		return
	}

	// Fire-and-forget notification fan-out (Rule 3).
	s.fanoutObjectEvent(bucket, *obj, "OBJECT_FINALIZE")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(obj)
}

func (s *Service) handleResumableInit(w http.ResponseWriter, r *http.Request, bucket string) {
	name := r.URL.Query().Get("name")

	// The object name can also come from the JSON body.
	if name == "" {
		var meta struct {
			Name string `json:"name"`
		}
		json.NewDecoder(r.Body).Decode(&meta)
		name = meta.Name
	}

	if name == "" {
		writeBadRequest(w, "Object name is required")
		return
	}

	uploadID := generateEtag()

	s.resumableMu.Lock()
	s.resumables[uploadID] = &resumableUpload{
		Bucket:      bucket,
		Name:        name,
		ContentType: r.Header.Get("X-Upload-Content-Type"),
	}
	s.resumableMu.Unlock()

	// Return the upload URI in the Location header.
	location := fmt.Sprintf("%s?uploadType=resumable&upload_id=%s",
		r.URL.Path, uploadID)

	w.Header().Set("Location", location)
	w.WriteHeader(http.StatusOK)
}

func (s *Service) handleResumableChunk(w http.ResponseWriter, r *http.Request, bucket string) {
	uploadID := r.URL.Query().Get("upload_id")
	if uploadID == "" {
		writeBadRequest(w, "upload_id is required")
		return
	}

	s.resumableMu.Lock()
	ru, ok := s.resumables[uploadID]
	s.resumableMu.Unlock()

	if !ok {
		writeNotFound(w, "Upload session not found")
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internalError", "Failed to read body")
		return
	}
	ru.Data.Write(data)

	// For simplicity, we treat every PUT as a complete upload.
	// Real GCS supports chunked resumable uploads with Content-Range headers,
	// but most client libraries send the entire content in one PUT.
	contentType := ru.ContentType
	if contentType == "" {
		contentType = r.Header.Get("Content-Type")
	}

	obj, err := s.store.PutObject(ru.Bucket, ru.Name, contentType, ru.Data.Bytes())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internalError", err.Error())
		return
	}

	s.resumableMu.Lock()
	delete(s.resumables, uploadID)
	s.resumableMu.Unlock()

	// Fire-and-forget OBJECT_FINALIZE notification fan-out (Rule 3).
	s.fanoutObjectEvent(ru.Bucket, *obj, "OBJECT_FINALIZE")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(obj)
}

// --- Middleware and default handler ---

// handleSign generates a signed URL for a given bucket/object.
// POST /_localgcp/sign with JSON body {"bucket":"b","object":"o","expires":3600}
// Returns {"signedUrl":"http://host/b/o?X-Goog-Signature=localgcp&X-Goog-Expires=3600"}
//
// This is a convenience endpoint. The Go SDK's storage.SignedURL() works client-side
// with the dummy credentials from internal/auth without hitting this endpoint.
// This endpoint exists for non-Go clients that need server-side URL generation.
func (s *Service) handleSign(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "methodNotAllowed", "Method not allowed")
		return
	}

	var req struct {
		Bucket  string `json:"bucket"`
		Object  string `json:"object"`
		Expires int    `json:"expires"` // seconds
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "Invalid JSON body")
		return
	}
	if req.Bucket == "" || req.Object == "" {
		writeError(w, http.StatusBadRequest, "invalid", "bucket and object are required")
		return
	}
	if req.Expires <= 0 {
		req.Expires = 3600
	}

	// Build signed URL pointing to this emulator's XML API path.
	scheme := "http"
	host := r.Host
	signedURL := fmt.Sprintf("%s://%s/%s/%s?X-Goog-Signature=localgcp&X-Goog-Expires=%d",
		scheme, host, req.Bucket, req.Object, req.Expires)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"signedUrl": signedURL,
	})
}

func (s *Service) handleDefault(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"kind":    "storage#serviceAccount",
			"service": "localgcp",
		})
		return
	}

	// XML API style: GET /{bucket}/{object} — used by the Go storage client for downloads.
	// Path format: /{bucket}/{object...} where object can contain slashes.
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if idx := strings.Index(path, "/"); idx > 0 {
			bucket := path[:idx]
			objectName := path[idx+1:]
			if objectName != "" {
				if _, ok := s.store.GetBucket(bucket); ok {
					meta, content, ok := s.store.GetObject(bucket, objectName)
					if ok {
						w.Header().Set("Content-Type", meta.ContentType)
						w.Header().Set("Content-Length", meta.Size)
						w.Header().Set("X-Goog-Hash", fmt.Sprintf("md5=%s", meta.Md5Hash))
						w.Header().Set("Accept-Ranges", "bytes")
						w.Header().Set("Etag", meta.Etag)
						if r.Method == http.MethodHead {
							w.WriteHeader(http.StatusOK)
							return
						}
						w.WriteHeader(http.StatusOK)
						w.Write(content)
						return
					}
				}
			}
		}
	}

	writeNotFound(w, fmt.Sprintf("Path not found: %s", r.URL.Path))
}

func (s *Service) loggingMiddleware(next http.Handler) http.Handler {
	if s.quiet {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rw, r)
		s.logger.Printf("%s %s %d %s",
			r.Method, r.URL.Path, rw.statusCode,
			time.Since(start).Round(time.Millisecond))
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
