package main

import (
	pb "handin5/grpc"
	"fmt"

	"google.golang.org/grpc"
)

type FE struct {
	pb.UnimplementedFEServer
	replicas map[string]pb.AuctionClient
}

func main() {

}