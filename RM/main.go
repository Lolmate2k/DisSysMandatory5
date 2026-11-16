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
}

func newRMServer(ip string, id int64) *RM {
	return &RM{
		replicas: make(map[string]pb.RMClient),
		deadReplicas: make(map[string]pb.RMClient),

		ip: ip,
		id: id,
		lamport: 0,
		highestBid: BID{},
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

func (n *RM) Bid(pbBid *pb.Amount, ctx context.Context) (*pb.Ack) {
	
}

func (n *RM) Result(pbEmpty *pb.Empty) (*pb.Outcome) {

}

// Can only be called by leader
func (n *RM) UpdateReplicas(pbCurrentStae *pb.CurrentState) (*pb.Ack) {

}

func (n *RM) Ping(pbEmpty *pb.Empty) (*pb.Empty) {

}

func (n *RM) Election(pbElection *pb.ElectionMessage) (*pb.ElectionResult) {

}




