package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"

	eventpb "github.com/incidentassistant/eventhub/proto/event"
)

type server struct {
	eventpb.UnimplementedEventServiceServer
}

func (s *server) EmitEvent(ctx context.Context, in *eventpb.EventMessage) (*eventpb.EventResponse, error) {
	// Log the received data
	log.Printf("Received event:\nNamespace: %s\nResourceKey: %s\nEventType: %s\nData: %s\nApiKey: %s\n",
		in.Namespace, in.ResourceKey, in.EventType, string(in.Data), in.ApiKey)

	return &eventpb.EventResponse{Acknowledged: true}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	eventpb.RegisterEventServiceServer(s, &server{})

	log.Printf("Server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
