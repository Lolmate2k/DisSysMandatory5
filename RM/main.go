package main

import (
	"bufio"
	"flag"
	"fmt"
	pb "handin5/grpc"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type BID struct {
	bidder string
	bid int64
}

type RM struct {
	pb.UnimplementedRMServer
	replicas map[string]pb.RMClient
	deadReplicas map[string]pb.RMClient

	ip string
	id int64
	mu sync.Mutex
	lamport int64
	highestBid BID
	closed bool
}

func newRMServer(ip string, id int64) *RM {
	return &RM{
		replicas: make(map[string]pb.RMClient),
		deadReplicas: make(map[string]pb.RMClient),

		ip: ip,
		id: id,
		lamport: 0,
		highestBid: BID{},
		closed: true,
	}
}

func main() {
	RMid := ParseAndLog()

	ip, addrs := FindFriendsAndSelf(RMid)

	// starting the RM (important shitz)
	lis, err := net.Listen("tcp", fmt.Sprint(ip))
	if err != nil {
		log.Fatalf("[RM %d] event=LISTEN_FAILED addr=%s err=%v", RMid, ip, err)
	}
	grpcServer := grpc.NewServer()
	n := newRMServer(ip, RMid)
	pb.RegisterRMServer(grpcServer, n)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("[RM %d] event=SERVE_FAILED err=%v", RMid, err)
		}
	}()

	var conns []*grpc.ClientConn
	for _, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("[RM %d] event=CLIENT_CREATE_FAILED peer=%s err=%v", n.id, addr, err)
		}
		conns = append(conns, conn)
		n.replicas[addr] = pb.NewRMClient(conn)
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	log.Printf("[Node %d] event=NODE_START addr=%s my_lamport=%d", n.id, n.ip, n.lamport)


}



func ParseAndLog() int64{
	RMid := flag.Int64("RMid", -1, "the id of the RM")
	flag.Parse()
	if *RMid <= -1 || *RMid >= 2 {
		log.Fatal("Could not start")
	}

	// Setup logging to a log file
	logFile, err := os.OpenFile(fmt.Sprintf("logs/client_%d.log", *RMid), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	return *RMid
}

func FindFriendsAndSelf(RMid int64) (string, []string) {
	// OPening RM file
	replicaFile, err := os.Open("rm.txt")
	if err != nil {
		log.Fatalf("[RM %d] event=PEER_FILE_OPEN_FAILED err=%v", RMid, err)
	}

	var addrs []string
	scanner := bufio.NewScanner(replicaFile)

	count := int64(0)
	ip := ""
	
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if count == RMid {
			ip = line
		} else {
			addrs = append(addrs, line)
		}
		count++
	}

	if scanner.Err() != nil {
		log.Fatalf("[RM %d] event=PEER_FILE_SCAN_FAILED err=%v", RMid, scanner.Err())
	}
	if ip == "" {
		log.Fatalf("[RM %d] event=INVALID_PROCESS_ID reason=out_of_range", RMid)
	}
	return ip, addrs
}




// GRPC Methods

func (n *RM) Bid(ctx context.Context, pbBid *pb.Amount) (*pb.Ack, error) {
	bid := pbBid.Dollars
	lamport := pbBid.Lamport
	bidder := pbBid.Bidder

	if bidder == n.highestBid.bidder {
		var returnMessage = fmt.Sprintf("You currently have the highest bid at: %d", n.highestBid.bid)
		return &pb.Ack{
			Message: returnMessage,
			Lamport: n.lamport,
		}, nil
	}
	if bid <= n.highestBid.bid {
		var returnMessage = fmt.Sprintf("Your bid is lower than highest bid, which is: %d", n.highestBid.bid)
		return &pb.Ack{
			Message: returnMessage,
			Lamport: n.lamport,
		}, nil
	}
	
	n.lamport = max(n.lamport, lamport) + 1
	
	log.Printf("[RM %d] event=BID_RECIEVED bid=%d bidder=%s", n.id, bid, bidder)

	state := &pb.CurrentState{
		HighestBid: bid,
		Bidder: bidder,
		Closed: false,
		Lamport: n.lamport,
	}

	if !n.ReplicateToMajority(state) {
		return &pb.Ack{Message: "Your bid was not accepted beacause of an internal server failure", Lamport: n.lamport}, nil
	}
	
	n.highestBid = BID{bidder: bidder, bid: bid}

	return &pb.Ack{Message: "Successfully bidded", Lamport: n.lamport}, nil
}

func (n *RM) Result(ctx context.Context, pbRead *pb.Read) (*pb.Outcome, error) {
	n.lamport = max(pbRead.Lamport, n.lamport) + 1
	
	return &pb.Outcome{
		Winner: n.highestBid.bidder,
		BuyingPrice: n.highestBid.bid,
		Lamport: n.lamport,
		Closed: n.closed,
	}, nil
}

// Can only be called by leader
func (n *RM) UpdateReplicas(ctx context.Context, in *pb.CurrentState) (*pb.Ack, error) {

}

func (n *RM) Ping(ctx context.Context, in *pb.Empty) (*pb.Empty, error) {

}

func (n *RM) Election(ctx context.Context, in *pb.ElectionMessage) (*pb.ElectionResult, error) {

}

func (n *RM) ReplicateToMajority(state *pb.CurrentState) bool{
	total := len(n.replicas) + 1
	needed := total/2 + 1
	acks := 1

	results := make(chan bool, len(n.replicas))

	for addr, client := range n.replicas {
		go func(addr string, client pb.RMClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := client.UpdateReplicas(ctx, state)
			if err != nil {
				log.Printf("[RM %d] event=REPLICATE_FAILED peer=%s err=%v", n.id, addr, err)
			}
			results <- (err == nil)
		}(addr, client)
	}

	remaining := len(n.replicas)
	for remaining > 0 {
		if <-results {
			acks++
		}
		remaining--

		if acks >= needed {
			return true // majority reached, don't wait for the rest
		}
		if acks+remaining < needed {
			return false // majority can no longer be reached
		}
	}
	return false
}




