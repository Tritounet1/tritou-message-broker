package api

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	tp "tidy/topic"

	"google.golang.org/grpc"
)

// main
func RunServer() {
	const dataDir = "./data"
	const maxSegSize = 10 * 1024 * 1024 // 10 MB par segment, à tuner

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	broker, err := newBrokerServer(dataDir, maxSegSize)
	if err != nil {
		log.Fatalf("failed to init broker: %v", err)
	}

	s := grpc.NewServer()
	tp.RegisterBrokerServer(s, broker)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	<-stop
	log.Println("shutting down gracefully...")
	s.GracefulStop()

	// Important : fermer les logs pour flush les buffers !
	for name, t := range broker.topics {
		if err := t.log.Close(); err != nil {
			log.Printf("error closing log %s: %v", name, err)
		}
	}
	log.Println("server stopped")
}
