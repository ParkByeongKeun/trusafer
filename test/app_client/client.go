package main

import (
	"log"
	"sync"

	pb "app_client/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	address = "localhost:9000"
)

var c pb.MainControlClient
var wg sync.WaitGroup

func main() {
	opts := grpc.WithInsecure()
	certFile := "cert/rootca.crt"
	creds, sslErr := credentials.NewClientTLSFromFile(certFile, "")
	if sslErr != nil {
		log.Fatalf("Error while loading CA trust certificate: %v", sslErr)
		return
	}
	opts = grpc.WithTransportCredentials(creds)
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, opts)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c = pb.NewMainControlClient(conn)

	// wg.Add(1)
	// go testGetBarn()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go testAddRegisterer()

		wg.Add(1)
		go testUpdateRegistererStatus(6, pb.RegistererStatus_REGISTERER_STATUS_BLOCKED)

		wg.Add(1)
		go testDeleteRegisterer(23) //registerer id

		wg.Add(1)
		go testGetRegisterer(1) //registerer id

		wg.Add(1)
		go testGetRegistererList(1) //barn id

		wg.Add(1)
		go testGetBarn()

		wg.Add(1)
		go testGetBarnList()

		wg.Add(1)
		go testGetAccessLog(1) //barn id

		wg.Add(1)
		go testOpenGateFromApp(1) //barn id
	}

	wg.Wait()
}
