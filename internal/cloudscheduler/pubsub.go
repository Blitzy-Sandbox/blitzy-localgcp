// Package cloudscheduler — pubsub.go provides the loopback Pub/Sub publish
// helper used by PubsubTarget dispatch.
//
// The helper opens a short-lived gRPC client to the localgcp Pub/Sub emulator
// at pubsubAddr, publishes a single message, and closes the connection. A
// short-lived connection is acceptable because scheduler ticks happen at
// minute-or-slower granularity — connection pooling is not warranted.
package cloudscheduler

import (
	"context"
	"fmt"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// pubsubPublishTimeout bounds each loopback Pub/Sub publish call made by
// Cloud Scheduler's PubsubTarget dispatch. The 30-second budget matches
// AAP §0.1.1 Extension C's PubsubTarget-via-loopback-gRPC contract and
// aligns with the 30-second Timeout used by internal/dispatch.Dispatcher
// for HTTP-target delivery. Using a 5-second budget (as in earlier
// iterations) could cause spurious timeouts under load or when Pub/Sub
// is temporarily slow, producing silent missed dispatches since the
// publish path is fire-and-forget.
const pubsubPublishTimeout = 30 * time.Second

// publishToPubsub opens a gRPC client to pubsubAddr and publishes one message
// to topic with the given data and attributes. Returns the first error
// encountered.
func publishToPubsub(pubsubAddr, topic string, data []byte, attributes map[string]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), pubsubPublishTimeout)
	defer cancel()

	conn, err := grpc.NewClient(pubsubAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", pubsubAddr, err)
	}
	defer conn.Close()

	client := pubsubpb.NewPublisherClient(conn)

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
