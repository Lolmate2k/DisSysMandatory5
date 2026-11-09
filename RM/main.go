package main

import (
	pb "handin5/grpc"
	"sync"
)

type BID struct {
	bidder string
	bid int64
}

type RM struct {
	pb.UnimplementedAuctionServer
	replicas map[string]pb.AuctionClient
	deadReplicas map[string]pb.AuctionClient

	ip string
	mu sync.Mutex
	lamport int64
	highestBid BID
}

func main() {

}