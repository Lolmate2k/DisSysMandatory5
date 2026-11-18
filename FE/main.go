package main

import (
	"bufio"
	"context"
	"flag"
	pb "handin5/grpc"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type BID struct {
	bidder string
	bid    int64
}

type FE struct {
	pb.UnimplementedFEServer
	replicas map[string]pb.RMClient
	rmLeader string

	mu sync.Mutex
	highestBid BID
	closed bool
	lamport int64
}

func main() {
	addr := flag.String("addr", "localhost:5000", "address the frontend listens on")
	flag.Parse()

	logFile, err := os.OpenFile("../logs/FE.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	log.SetOutput(logFile)


	fe := &FE{replicas: make(map[string]pb.RMClient)}

	f, err := os.Open("../rm.txt")
	if err != nil {
		log.Fatalf("[FE] event=RM_FILE_OPEN_FAILED err=%v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		conn, err := grpc.NewClient(line, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("[FE] event=CLIENT_CREATE_FAILED rm=%s err=%v", line, err)
		}
		defer conn.Close()
		fe.replicas[line] = pb.NewRMClient(conn)
	}
	if scanner.Err() != nil {
		log.Fatalf("[FE] event=RM_FILE_SCAN_FAILED err=%v", scanner.Err())
	}

	lis, err := net.Listen("tcp", *addr) 
	if err != nil {
		log.Fatalf("[FE] event=LISTEN_FAILED addr=%s err=%v", *addr, err)
	}
	s := grpc.NewServer()
	pb.RegisterFEServer(s, fe)
	log.Printf("[FE] event=SERVER_START addr=%s", *addr)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("[FE event=SERVE_FAILED err=%v", err)
	}


}

func (n *FE) Bid(ctx context.Context, pbBid *pb.Amount) (*pb.Ack, error) {
	n.mu.Lock()
	n.lamport = max(n.lamport, pbBid.Lamport) + 1
	req := &pb.Amount{Dollars: pbBid.Dollars, Lamport: n.lamport, Bidder: pbBid.Bidder}
	n.mu.Unlock()

	// Get leader address
	addr, leader := n.GetLeader()
	if leader == nil {
		return nil, status.Error(codes.Unavailable, "no leader known")
	}

	// Call bid to leader
	callCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ack, err := leader.Bid(callCtx, req)
	if err != nil {
		log.Printf("[FE] event=BID_FAILED leader=%s err=%v", addr, err)
		n.ForgetLeader(addr)
		return nil, err
	}
	
	n.mu.Lock()
	n.lamport = max(n.lamport, ack.Lamport) +1
	ack.Lamport = n.lamport
	n.mu.Unlock()
	return ack, nil
}

func (n *FE) GetLeader() (string, pb.RMClient) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.rmLeader == "" {
		return "", nil
	}
	return n.rmLeader, n.replicas[n.rmLeader]
} 

func (n *FE) ForgetLeader(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.rmLeader == addr {
		n.rmLeader = ""
	}
}

func (n *FE) Result(ctx context.Context, pbRead *pb.Read) (*pb.Outcome, error) {
	n.mu.Lock()
	n.lamport = max(n.lamport, pbRead.Lamport) + 1
	req := &pb.Read{Lamport: n.lamport}
	n.mu.Unlock()
	
	addr, leader := n.GetLeader()
	if leader == nil {
		return nil, status.Error(codes.Unavailable, "no leader known")
	}

	callCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, err := leader.Result(callCtx, req)
	if err != nil {
		log.Printf("[FE] event=RESULT_GET_FAILED leader=%s err=%v", addr, err)
		n.ForgetLeader(addr)
		return nil, err
	}
	
	n.mu.Lock()
	n.lamport = max(n.lamport, out.Lamport) +1
	out.Lamport = n.lamport
	n.mu.Unlock()

	return out, nil
}

func (n *FE) LeaderUpdate(ctx context.Context, pbElectionResult *pb.ElectionResult) (*pb.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, ok := n.replicas[pbElectionResult.WinnerIp]; !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown candidate %s", pbElectionResult.WinnerIp)
	}

	n.rmLeader = pbElectionResult.WinnerIp
	log.Printf("[FE] event=LEADER_UPDATE leader=%s", n.rmLeader)
	return &pb.Empty{}, nil
}



