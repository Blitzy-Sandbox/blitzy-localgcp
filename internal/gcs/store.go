package gcs

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Bucket represents a GCS bucket.
type Bucket struct {
	Kind         string `json:"kind"`
	ID           string `json:"id"`
	Name         string `json:"name"`
	Location     string `json:"location"`
	StorageClass string `json:"storageClass"`
	TimeCreated  string `json:"timeCreated"`
	Updated      string `json:"updated"`
	Etag         string `json:"etag"`
}

// Object represents a GCS object (metadata only, content stored separately).
type Object struct {
	Kind            string `json:"kind"`
	ID              string `json:"id"`
	Name            string `json:"name"`
	Bucket          string `json:"bucket"`
	Size            string `json:"size"`
	ContentType     string `json:"contentType"`
	TimeCreated     string `json:"timeCreated"`
	Updated         string `json:"updated"`
	Md5Hash         string `json:"md5Hash"`
	Crc32c          string `json:"crc32c"`
	Etag            string `json:"etag"`
	ContentEncoding string `json:"contentEncoding,omitempty"`
	SelfLink        string `json:"selfLink,omitempty"`
}

// NotificationConfig is a Cloud Storage notification configuration that
// routes object events to a Pub/Sub topic. See AAP Extension B.
//
// The TopicName field is a full resource name in the form
// `projects/{project}/topics/{topic}` — the format produced by the
// Cloud Storage API when using the JSON representation. Its JSON tag
// remains "topic" so the on-the-wire representation matches the
// canonical GCS notification config schema exactly.
//
// The struct is intentionally defined as a value type so both Store
// fan-out paths (snapshot copies in NotificationsForBucket) and the
// HTTP handler responses in service.go can marshal without leaking
// pointer aliases to internal state.
type NotificationConfig struct {
	// Kind is the resource kind, always "storage#notification".
	Kind string `json:"kind"`

	// ID is the per-bucket unique identifier assigned by the Store at
	// creation time.
	ID string `json:"id"`

	// TopicName is the fully-qualified Pub/Sub topic name that object
	// event notifications are published to, typically of the form
	// "projects/{project}/topics/{topic}". The JSON key remains "topic"
	// for wire-format parity with the real GCS notification config API.
	TopicName string `json:"topic"`

	// PayloadFormat controls what body is sent to the topic. The
	// emulator produces "JSON_API_V1" only; "NONE" is accepted for
	// round-trip compatibility.
	PayloadFormat string `json:"payload_format"`

	// EventTypes is the list of object event types this config
	// subscribes to. Supported values: "OBJECT_FINALIZE",
	// "OBJECT_DELETE". Empty means "all supported events" per GCS
	// convention, which the service layer expands at fan-out time.
	EventTypes []string `json:"event_types,omitempty"`

	// CustomAttributes carries extra Pub/Sub message attributes that
	// are merged on top of the standard {eventType, bucketId} attrs at
	// delivery time.
	CustomAttributes map[string]string `json:"custom_attributes,omitempty"`

	// ObjectNamePrefix filters which object names trigger this
	// notification. An empty string matches all objects.
	ObjectNamePrefix string `json:"object_name_prefix,omitempty"`

	// Etag is the server-assigned version identifier for optimistic
	// concurrency, set on create.
	Etag string `json:"etag,omitempty"`

	// SelfLink is the fully-qualified URL to this notification config,
	// set on create.
	SelfLink string `json:"selfLink,omitempty"`
}

// NotificationList is the response for ListNotifications.
type NotificationList struct {
	Kind  string               `json:"kind"` // "storage#notifications"
	Items []NotificationConfig `json:"items"`
}

// BucketList is the response for listing buckets.
type BucketList struct {
	Kind  string   `json:"kind"`
	Items []Bucket `json:"items"`
}

// ObjectList is the response for listing objects.
type ObjectList struct {
	Kind     string   `json:"kind"`
	Items    []Object `json:"items"`
	Prefixes []string `json:"prefixes,omitempty"`
}

// Store is the storage backend for the GCS emulator.
type Store struct {
	mu      sync.RWMutex
	buckets map[string]*Bucket
	objects map[string]map[string]*storedObject // bucket -> object name -> object

	// notifications is the per-bucket notification-config registry used
	// by GCS→Pub/Sub fan-out. Keyed by bucket name then by config id.
	notifications map[string]map[string]*NotificationConfig

	// notifCounter generates per-store unique notification config ids.
	notifCounter uint64

	dataDir string
}

type storedObject struct {
	Meta    Object
	Content []byte
}

// NewStore creates a new GCS store. If dataDir is non-empty, it loads
// persisted state and flushes on writes.
func NewStore(dataDir string) *Store {
	s := &Store{
		buckets:       make(map[string]*Bucket),
		objects:       make(map[string]map[string]*storedObject),
		notifications: make(map[string]map[string]*NotificationConfig),
		dataDir:       dataDir,
	}
	if dataDir != "" {
		s.load()
	}
	return s
}

// --- Bucket operations ---

func (s *Store) CreateBucket(name, project string) (*Bucket, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.buckets[name]; exists {
		return nil, fmt.Errorf("conflict: bucket %q already exists", name)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	b := &Bucket{
		Kind:         "storage#bucket",
		ID:           name,
		Name:         name,
		Location:     "US",
		StorageClass: "STANDARD",
		TimeCreated:  now,
		Updated:      now,
		Etag:         generateEtag(),
	}
	s.buckets[name] = b
	s.objects[name] = make(map[string]*storedObject)
	s.notifications[name] = make(map[string]*NotificationConfig)
	s.persist()
	return b, nil
}

func (s *Store) GetBucket(name string) (*Bucket, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.buckets[name]
	return b, ok
}

func (s *Store) ListBuckets() []Bucket {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]Bucket, 0, len(s.buckets))
	for _, b := range s.buckets {
		result = append(result, *b)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

func (s *Store) DeleteBucket(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.buckets[name]; !exists {
		return fmt.Errorf("not found: bucket %q", name)
	}
	if objs, ok := s.objects[name]; ok && len(objs) > 0 {
		return fmt.Errorf("conflict: bucket %q is not empty", name)
	}

	delete(s.buckets, name)
	delete(s.objects, name)
	delete(s.notifications, name)
	s.persist()
	return nil
}

// --- Notification configuration operations ---
//
// NotificationConfigs are scoped to a bucket and identified by a
// per-bucket integer id encoded as a string. Create returns the
// populated config with ID set; the other methods are standard
// get/list/delete semantics.

// CreateNotification registers a new notification configuration for
// `bucket`. Returns a populated copy (Kind, ID, Etag, SelfLink) or an
// error if the bucket does not exist.
func (s *Store) CreateNotification(bucket string, cfg NotificationConfig) (*NotificationConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.buckets[bucket]; !exists {
		return nil, fmt.Errorf("not found: bucket %q", bucket)
	}
	if _, ok := s.notifications[bucket]; !ok {
		s.notifications[bucket] = make(map[string]*NotificationConfig)
	}
	s.notifCounter++
	cfg.ID = fmt.Sprintf("%d", s.notifCounter)
	cfg.Kind = "storage#notification"
	cfg.Etag = generateEtag()
	cfg.SelfLink = fmt.Sprintf("https://storage.googleapis.com/storage/v1/b/%s/notificationConfigs/%s", bucket, cfg.ID)
	if cfg.PayloadFormat == "" {
		cfg.PayloadFormat = "JSON_API_V1"
	}
	if len(cfg.EventTypes) == 0 {
		// Empty means "all event types" per GCS convention. Callers that
		// want an explicit subset may specify OBJECT_FINALIZE / OBJECT_DELETE.
		cfg.EventTypes = nil
	}
	stored := cfg
	s.notifications[bucket][cfg.ID] = &stored
	s.persist()
	return &stored, nil
}

// GetNotification returns the notification config with the given id,
// or (nil, false) if no such config exists.
func (s *Store) GetNotification(bucket, id string) (*NotificationConfig, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byID, ok := s.notifications[bucket]
	if !ok {
		return nil, false
	}
	c, ok := byID[id]
	if !ok {
		return nil, false
	}
	// Return a defensive copy so callers cannot mutate store state.
	copied := *c
	return &copied, true
}

// ListNotifications returns all configs for the bucket, sorted by id.
// Returns nil (empty list) if the bucket does not exist or has no
// configs. The second return value is true when the bucket itself
// exists (so callers can distinguish "no configs" from "no bucket").
func (s *Store) ListNotifications(bucket string) ([]NotificationConfig, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, exists := s.buckets[bucket]; !exists {
		return nil, false
	}
	byID := s.notifications[bucket]
	out := make([]NotificationConfig, 0, len(byID))
	for _, c := range byID {
		out = append(out, *c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, true
}

// DeleteNotification removes the named config. Returns an error if the
// config does not exist.
func (s *Store) DeleteNotification(bucket, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	byID, ok := s.notifications[bucket]
	if !ok {
		return fmt.Errorf("not found: bucket %q", bucket)
	}
	if _, ok := byID[id]; !ok {
		return fmt.Errorf("not found: notification %q in bucket %q", id, bucket)
	}
	delete(byID, id)
	s.persist()
	return nil
}

// NotificationsForBucket returns a snapshot copy of the configs for
// the bucket. Used by the fan-out path to avoid holding the store
// lock while publishing to Pub/Sub.
func (s *Store) NotificationsForBucket(bucket string) []NotificationConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byID := s.notifications[bucket]
	out := make([]NotificationConfig, 0, len(byID))
	for _, c := range byID {
		out = append(out, *c)
	}
	return out
}

// --- Object operations ---

func (s *Store) PutObject(bucket, name, contentType string, content []byte) (*Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.buckets[bucket]; !exists {
		return nil, fmt.Errorf("not found: bucket %q", bucket)
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}

	md5sum := md5.Sum(content)
	sha256sum := sha256.Sum256(content)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	obj := &Object{
		Kind:        "storage#object",
		ID:          fmt.Sprintf("%s/%s", bucket, name),
		Name:        name,
		Bucket:      bucket,
		Size:        fmt.Sprintf("%d", len(content)),
		ContentType: contentType,
		TimeCreated: now,
		Updated:     now,
		Md5Hash:     base64.StdEncoding.EncodeToString(md5sum[:]),
		Crc32c:      "AAAAAA==",
		Etag:        hex.EncodeToString(sha256sum[:8]),
		// SelfLink is the canonical JSON API URL for the object per
		// the GCS REST schema. It is required by AAP §0.1.1 for the
		// notification payload to include all 8 canonical fields
		// {kind, id, selfLink, name, bucket, contentType, timeCreated,
		// updated}. The path segment is percent-encoded because object
		// names may contain spaces, slashes, Unicode, and reserved
		// characters (see TestNotification special-character suite).
		SelfLink: fmt.Sprintf("https://www.googleapis.com/storage/v1/b/%s/o/%s", bucket, url.PathEscape(name)),
	}

	s.objects[bucket][name] = &storedObject{Meta: *obj, Content: content}
	s.persist()
	return obj, nil
}

func (s *Store) GetObject(bucket, name string) (*Object, []byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	objs, ok := s.objects[bucket]
	if !ok {
		return nil, nil, false
	}
	so, ok := objs[name]
	if !ok {
		return nil, nil, false
	}
	return &so.Meta, so.Content, true
}

func (s *Store) DeleteObject(bucket, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	objs, ok := s.objects[bucket]
	if !ok {
		return fmt.Errorf("not found: bucket %q", bucket)
	}
	if _, ok := objs[name]; !ok {
		return fmt.Errorf("not found: object %q in bucket %q", name, bucket)
	}

	delete(objs, name)
	s.persist()
	return nil
}

func (s *Store) CopyObject(srcBucket, srcName, dstBucket, dstName string) (*Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.buckets[dstBucket]; !exists {
		return nil, fmt.Errorf("not found: bucket %q", dstBucket)
	}

	srcObjs, ok := s.objects[srcBucket]
	if !ok {
		return nil, fmt.Errorf("not found: bucket %q", srcBucket)
	}
	src, ok := srcObjs[srcName]
	if !ok {
		return nil, fmt.Errorf("not found: object %q in bucket %q", srcName, srcBucket)
	}

	// Make an independent copy of the content.
	content := make([]byte, len(src.Content))
	copy(content, src.Content)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	obj := src.Meta
	obj.ID = fmt.Sprintf("%s/%s", dstBucket, dstName)
	obj.Name = dstName
	obj.Bucket = dstBucket
	obj.TimeCreated = now
	obj.Updated = now
	// Rebuild SelfLink for the destination so the notification payload
	// emitted by the fan-out path carries the correct canonical URL
	// (AAP §0.1.1). The source's SelfLink would point at the source
	// bucket/object, which is incorrect for a copied object.
	obj.SelfLink = fmt.Sprintf("https://www.googleapis.com/storage/v1/b/%s/o/%s", dstBucket, url.PathEscape(dstName))

	s.objects[dstBucket][dstName] = &storedObject{Meta: obj, Content: content}
	s.persist()
	return &obj, nil
}

// ListObjects lists objects in a bucket with optional prefix and delimiter filtering.
func (s *Store) ListObjects(bucket, prefix, delimiter string, maxResults int) ([]Object, []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	objs, ok := s.objects[bucket]
	if !ok {
		return nil, nil
	}

	var items []Object
	prefixSet := make(map[string]struct{})

	for name, so := range objs {
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}

		if delimiter != "" {
			// Check if there's a delimiter after the prefix.
			rest := name[len(prefix):]
			idx := strings.Index(rest, delimiter)
			if idx >= 0 {
				// This object is "inside" a pseudo-directory. Add the prefix, skip the object.
				commonPrefix := prefix + rest[:idx+len(delimiter)]
				prefixSet[commonPrefix] = struct{}{}
				continue
			}
		}

		items = append(items, so.Meta)
	}

	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })

	if maxResults > 0 && len(items) > maxResults {
		items = items[:maxResults]
	}

	var prefixes []string
	for p := range prefixSet {
		prefixes = append(prefixes, p)
	}
	sort.Strings(prefixes)

	return items, prefixes
}

// --- Persistence ---

type persistedState struct {
	Buckets       []Bucket                        `json:"buckets"`
	Objects       map[string][]persistedObj       `json:"objects"`
	Notifications map[string][]NotificationConfig `json:"notifications,omitempty"`
	NotifCounter  uint64                          `json:"notifCounter,omitempty"`
}

type persistedObj struct {
	Meta    Object `json:"meta"`
	Content string `json:"content"` // base64 encoded
}

func (s *Store) persist() {
	if s.dataDir == "" {
		return
	}

	dir := filepath.Join(s.dataDir, "gcs")
	os.MkdirAll(dir, 0o755)

	state := persistedState{
		Buckets:       make([]Bucket, 0, len(s.buckets)),
		Objects:       make(map[string][]persistedObj),
		Notifications: make(map[string][]NotificationConfig),
		NotifCounter:  s.notifCounter,
	}

	for _, b := range s.buckets {
		state.Buckets = append(state.Buckets, *b)
	}

	for bucket, objs := range s.objects {
		var pObjs []persistedObj
		for _, so := range objs {
			pObjs = append(pObjs, persistedObj{
				Meta:    so.Meta,
				Content: base64.StdEncoding.EncodeToString(so.Content),
			})
		}
		state.Objects[bucket] = pObjs
	}

	for bucket, notifs := range s.notifications {
		list := make([]NotificationConfig, 0, len(notifs))
		for _, n := range notifs {
			list = append(list, *n)
		}
		state.Notifications[bucket] = list
	}

	data, _ := json.MarshalIndent(state, "", "  ")
	os.WriteFile(filepath.Join(dir, "state.json"), data, 0o644)
}

func (s *Store) load() {
	path := filepath.Join(s.dataDir, "gcs", "state.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return // No persisted state, start fresh.
	}

	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: corrupt data in %s, starting with empty state\n", path)
		return
	}

	for i := range state.Buckets {
		b := state.Buckets[i]
		s.buckets[b.Name] = &b
		if _, ok := s.objects[b.Name]; !ok {
			s.objects[b.Name] = make(map[string]*storedObject)
		}
	}

	for bucket, pObjs := range state.Objects {
		if _, ok := s.objects[bucket]; !ok {
			s.objects[bucket] = make(map[string]*storedObject)
		}
		for _, po := range pObjs {
			content, err := base64.StdEncoding.DecodeString(po.Content)
			if err != nil {
				continue
			}
			s.objects[bucket][po.Meta.Name] = &storedObject{Meta: po.Meta, Content: content}
		}
	}

	// Restore notification configs. A bucket that lost its notifications map on
	// load (e.g., because an older state.json omitted the field) gets an empty
	// map so subsequent CreateNotification calls succeed.
	for bucket := range s.buckets {
		if _, ok := s.notifications[bucket]; !ok {
			s.notifications[bucket] = make(map[string]*NotificationConfig)
		}
	}
	for bucket, notifs := range state.Notifications {
		if _, ok := s.notifications[bucket]; !ok {
			s.notifications[bucket] = make(map[string]*NotificationConfig)
		}
		for i := range notifs {
			n := notifs[i]
			s.notifications[bucket][n.ID] = &n
		}
	}
	if state.NotifCounter > s.notifCounter {
		s.notifCounter = state.NotifCounter
	}
}

// --- Helpers ---

var etagCounter uint64
var etagMu sync.Mutex

func generateEtag() string {
	etagMu.Lock()
	defer etagMu.Unlock()
	etagCounter++
	return fmt.Sprintf("CL%d=", etagCounter)
}
