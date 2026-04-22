// Package gcs — pubsub.go provides the loopback Pub/Sub publish helper used
// by the notification-config fan-out path.
//
// The helper opens a short-lived gRPC client to the localgcp Pub/Sub emulator
// at pubsubAddr, publishes a single message, and closes the connection.
//
// This file is consumed only by the notification fan-out goroutine in
// service.go. It is a no-op when `pubsubAddr` is empty (silently skipped
// per AAP Rule 7a) — the handler never blocks on the publish path (Rule 3).
package gcs

import (
	"context"
	"fmt"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// pubsubPublishTimeout caps the total time spent dialing + publishing.
// The dispatch goroutine is fire-and-forget, so a short, bounded timeout is
// sufficient — we never want a hung Pub/Sub service to leak goroutines.
const pubsubPublishTimeout = 5 * time.Second

// publishToPubsub opens a gRPC client to pubsubAddr and publishes one
// message to topic with the given data and attributes.
//
// Returns the first error encountered. Callers should NEVER propagate
// errors to the request handler — they MUST log to stderr only (Rule 3).
// Returns (nil, nil) immediately when pubsubAddr is empty.
func publishToPubsub(pubsubAddr, topic string, data []byte, attributes map[string]string) error {
	if pubsubAddr == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), pubsubPublishTimeout)
	defer cancel()

	conn, err := grpc.NewClient(pubsubAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", pubsubAddr, err)
	}
	defer conn.Close()

	client := pubsubpb.NewPublisherClient(conn)

	// Copy attributes defensively — callers may reuse the map after we
	// return, so the published message must own its own copy.
	attrs := make(map[string]string, len(attributes))
	for k, v := range attributes {
		attrs[k] = v
	}

	_, err = client.Publish(ctx, &pubsubpb.PublishRequest{
		Topic: topic,
		Messages: []*pubsubpb.PubsubMessage{
			{
				Data:       data,
				Attributes: attrs,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("publish to %s: %w", topic, err)
	}
	return nil
}
