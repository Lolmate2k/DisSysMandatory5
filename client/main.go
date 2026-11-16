package main

import (
	"flag"
	pb "handin5/grpc"
	"log"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type AuctionClient struct {
	client   pb.FEClient
	username string
	lamport  int64
	bank     int64
}

func newClient(username string, bank int64, conn *grpc.ClientConn) *AuctionClient {
	return &AuctionClient{
		client: pb.NewFEClient(conn),
		username: username,
		lamport: 0,
		bank: bank,
	}
}

func main() {

	name := flag.String("name", "", "your username")
	bank := flag.Int64("bank", 100, "Your balance (poor)")
	flag.Parse()
	if *name == "" || *bank <= 0 {
		os.Exit(1)
	}

	logFile, err := os.OpenFile("logs/client_"+*name+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	conn, err := grpc.NewClient("127.0.0.1:80085", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to server: %v", err)
	}
	defer conn.Close()


}
