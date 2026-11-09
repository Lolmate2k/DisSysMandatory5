package main 

import (
	pb "handin5/grpc"
)

type AuctionClient struct {
	client pb.AuctionClient
	username string
	lamport int64
	bank int64
	
}

func newClient(username string, bank int64, conn *grpc.) *AuctionClient

func main() {
	
}