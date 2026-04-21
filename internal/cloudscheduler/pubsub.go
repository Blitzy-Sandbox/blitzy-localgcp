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

// publishToPubsub opens a gRPC client to pubsubAddr and publishes one message
// to topic with the given data and attributes. Returns the first error
// encountered.
func publishToPubsub(pubsubAddr, topic string, data []byte, attributes map[string]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
