package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
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

type RM struct {
	pb.UnimplementedRMServer
	replicas     map[string]pb.RMClient
	deadReplicas map[string]pb.RMClient
	ids          map[string]int64
	fe           pb.FEClient

	ip              string
	id              int64
	mu              sync.Mutex
	bidMu           sync.Mutex
	lamport         int64
	highestBid      BID
	closed          bool
	isLeader        bool
	currentLeaderId int64
	lastPing        time.Time
	electing        bool
	auctionEnd      time.Time
}

const (
	pingInterval    = 500 * time.Millisecond
	pingTimeout     = 1000 * time.Millisecond
	leaderTimeout   = 2500 * time.Millisecond
	auctionDuration = 50 * time.Second
	totalReplicas = 3
)

func newRMServer(ip string, id int64) *RM {
	return &RM{
		replicas:     make(map[string]pb.RMClient),
		deadReplicas: make(map[string]pb.RMClient),
		ids:          make(map[string]int64),

		ip:         ip,
		id:         id,
		lamport:    0,
		highestBid: BID{},
		closed:     true,
		lastPing:   time.Now(),
		isLeader:   false,
		electing:   false,
	}
}

func main() {
	RMid, FeAddr := ParseAndLog()

	ip, addrs, ids := FindFriendsAndSelf(RMid)

	// starting the RM (important shitz)
	lis, err := net.Listen("tcp", fmt.Sprint(ip))
	if err != nil {
		log.Fatalf("[RM %d] event=LISTEN_FAILED addr=%s err=%v", RMid, ip, err)
	}
	grpcServer := grpc.NewServer()
	n := newRMServer(ip, RMid)
	n.ids = ids
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
			log.Fatalf("[RM %d] event=RM_CLIENT_CREATE_FAILED peer=%s err=%v", n.id, addr, err)
		}
		conns = append(conns, conn)
		n.replicas[addr] = pb.NewRMClient(conn)
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	feConn, err := grpc.NewClient(FeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[RM %d] event=FE_CLIENT_CREATE_FAILED address=%s err=%v", n.id, FeAddr, err)
	}
	defer feConn.Close()
	n.fe = pb.NewFEClient(feConn)

	log.Printf("[RM %d] event=RM_START addr=%s lamport=%d", n.id, n.ip, n.lamport)

	go n.HeartBeatLoop()
	
	select {}
}

func ParseAndLog() (int64, string) {
	RMid := flag.Int64("RMid", -1, "the id of the RM")
	feAddr := flag.String("fe", "localhost:5000", "address of the frontend")
	flag.Parse()
	if *RMid <= -1 || *RMid >= totalReplicas {
		log.Fatal("Could not start")
	}

	// Setup logging to a log file
	logFile, err := os.OpenFile(fmt.Sprintf("../logs/RM_%d.log", *RMid), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	log.SetOutput(logFile)

	return *RMid, *feAddr
}

func FindFriendsAndSelf(RMid int64) (string, []string, map[string]int64) {
	// OPening RM file
	replicaFile, err := os.Open("../rm.txt")
	if err != nil {
		log.Fatalf("[RM %d] event=PEER_FILE_OPEN_FAILED err=%v", RMid, err)
	}

	var addrs []string
	scanner := bufio.NewScanner(replicaFile)

	count := int64(0)
	ip := ""
	ids := make(map[string]int64)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if count == RMid {
			ip = line
		} else {
			ids[line] = count
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
	replicaFile.Close()
	return ip, addrs, ids
}

// GRPC Methods

func (n *RM) Bid(ctx context.Context, pbBid *pb.Amount) (*pb.Ack, error) {
	// Setting values for state
	bid := pbBid.Dollars
	bidder := pbBid.Bidder

	n.bidMu.Lock()
	defer n.bidMu.Unlock()

	n.mu.Lock()

	// Followers don't process bids
	if !n.isLeader {
		n.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "Not the leader")
	}

	n.lamport = max(n.lamport, pbBid.Lamport) + 1

	starting := n.auctionEnd.IsZero()

	// If the auction is over, the bid is rejected
	if !starting && (n.closed || !time.Now().Before(n.auctionEnd)) {
		lamport := n.lamport
		n.mu.Unlock()
		return &pb.Ack{Message: "The auction is closed", Lamport: lamport}, nil
	}

	// If the bidder sending a bid is currently the one with the highest bid
	if bidder == n.highestBid.bidder {
		var returnMessage = fmt.Sprintf("You currently have the highest bid at: %d", n.highestBid.bid)
		lamport := n.lamport
		n.mu.Unlock()
		return &pb.Ack{
			Message: returnMessage,
			Lamport: lamport,
		}, nil
	}

	// If the sent bid is lower than the current highest one
	if bid <= n.highestBid.bid {
		var returnMessage = fmt.Sprintf("Your bid is lower than highest bid, which is: %d", n.highestBid.bid)
		lamport := n.lamport
		n.mu.Unlock()
		return &pb.Ack{
			Message: returnMessage,
			Lamport: lamport,
		}, nil
	}

	end := n.auctionEnd
	if starting {
		end = time.Now().Add(auctionDuration)
	}

	//Log that the bid has been reciaved by the leader succesfully
	log.Printf("[RM %d] event=BID_RECIEVED bid=%d bidder=%s", n.id, bid, bidder)

	// Creates the state
	state := &pb.CurrentState{
		HighestBid: bid,
		Bidder:     bidder,
		Closed:     false,
		Lamport:    n.lamport,
		EndTime:    end.UnixNano(),
	}
	n.mu.Unlock()

	//If a majority does not repond
	if !n.ReplicateToMajority(state) {
		n.mu.Lock()
		lamport := n.lamport
		n.mu.Unlock()
		return &pb.Ack{Message: "Your bid was not accepted beacause of an internal server failure", Lamport: lamport}, nil
	}

	n.mu.Lock()
	n.highestBid = BID{bidder: bidder, bid: bid}
	if starting {
		n.auctionEnd = end
		n.closed = false
	}
	lamport := n.lamport
	n.mu.Unlock()

	if starting {
		log.Printf("[RM %d] event=AUCTION_STARTED end=%v", n.id, end)
		go n.AuctionTimer()
	}

	return &pb.Ack{Message: "Successfully bidded", Lamport: lamport}, nil
}

func (n *RM) Result(ctx context.Context, pbRead *pb.Read) (*pb.Outcome, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.isLeader {
		return nil, status.Error(codes.FailedPrecondition, "Not the leader")
	}

	n.lamport = max(pbRead.Lamport, n.lamport) + 1
	closed := n.closed || !time.Now().Before(n.auctionEnd)

	return &pb.Outcome{
		Winner:      n.highestBid.bidder,
		BuyingPrice: n.highestBid.bid,
		Lamport:     n.lamport,
		Closed:      closed,
	}, nil
}

func (n *RM) UpdateReplicas(ctx context.Context, in *pb.CurrentState) (*pb.Ack, error) { // Can only be called by leader
	n.mu.Lock()
	n.lamport = max(n.lamport, in.Lamport) + 1
	lamport := n.lamport
	n.auctionEnd = time.Unix(0, in.EndTime)
	n.closed = !time.Now().Before(n.auctionEnd)
	n.highestBid = BID{bidder: in.Bidder, bid: in.HighestBid}
	n.mu.Unlock()

	return &pb.Ack{Message: "[Replica] Successfully write current state", Lamport: lamport}, nil
}

func (n *RM) Ping(ctx context.Context, in *pb.Empty) (*pb.Empty, error) {
	n.mu.Lock()
	n.lastPing = time.Now()
	n.mu.Unlock()
	return &pb.Empty{}, nil
}

func (n *RM) Election(ctx context.Context, in *pb.ElectionMessage) (*pb.ElectionResult, error) {
	n.mu.Lock()

	if in.CandidateId > n.id {
		n.isLeader = false
		n.currentLeaderId = in.CandidateId
		n.electing = false
		n.lastPing = time.Now()
		n.mu.Unlock()
		log.Printf("[RM %d] event=NEW_LEADER leader=%d", n.id, in.CandidateId)
		return &pb.ElectionResult{WinnerIp: in.CandidateIp, WinnerId: in.CandidateId}, nil
	}

	startNew := !n.isLeader && !n.electing
	if startNew {
		n.electing = true
	}
	n.mu.Unlock()

	if startNew {
		go n.StartElection()
	}
	return &pb.ElectionResult{WinnerIp: n.ip}, nil
}

// Other methods

func (n *RM) StartElection() {
	defer func() {
		n.mu.Lock()
		n.electing = false
		n.mu.Unlock()
	}()

	log.Printf("[RM %d] event=ELECTION_STARTED", n.id)

	n.mu.Lock()
	higher := make(map[string]pb.RMClient)
	lower := make(map[string]pb.RMClient)
	classify := func(addr string, client pb.RMClient) {
		if n.ids[addr] > n.id {
			higher[addr] = client
		} else {
			lower[addr] = client
		}
	}
	for addr, client := range n.replicas {
		classify(addr, client)
	}
	for addr, client := range n.deadReplicas {
		classify(addr, client)
	}
	n.mu.Unlock()

	msg := &pb.ElectionMessage{CandidateIp: n.ip, CandidateId: n.id}

	results := make(chan bool, len(higher))

	for _, client := range higher {
		go func(client pb.RMClient) {
			ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
			defer cancel()
			_, err := client.Election(ctx, msg)
			results <- (err == nil)
		}(client)
	}
	for range higher {
		if <-results {
			n.mu.Lock()
			n.lastPing = time.Now()
			n.mu.Unlock()
			log.Printf("[RM %d] event=ELECTION_YIELD", n.id)
			return
		}
	}

	n.mu.Lock()
	n.isLeader = true
	n.currentLeaderId = n.id
	n.lastPing = time.Now()
	n.mu.Unlock()
	log.Printf("[RM %d] event=ELECTION_WON", n.id)
	n.NotifyFE()

	for addr, client := range lower {
		go func(addr string, client pb.RMClient) {
			ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
			defer cancel()
			_, err := client.Election(ctx, msg)
			if err != nil {
				log.Printf("[RM %d] event=ANNOUNCE_FAILED peer=%s error=%v", n.id, addr, err)
			}
		}(addr, client)
	}
}

func (n *RM) ReplicateToMajority(state *pb.CurrentState) bool {
	n.mu.Lock()
	peers := make(map[string]pb.RMClient, len(n.replicas))
	for a, c := range n.replicas {
		peers[a] = c
	}
	total := len(n.replicas) + 1
	n.mu.Unlock()
	needed := total/2 + 1
	acks := 1

	results := make(chan bool, len(peers))

	for addr, client := range peers {

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

	remaining := len(peers)
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

func (n *RM) HeartBeatLoop() {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for range ticker.C {
		n.mu.Lock()
		shouldElect := !n.isLeader && !n.electing && time.Since(n.lastPing) > leaderTimeout
		if shouldElect {
			n.electing = true
		}
		isLeader := n.isLeader
		n.mu.Unlock()

		if isLeader {
			n.pingReplicas()
			go n.NotifyFE()
		} else if shouldElect {
			log.Printf("[RM %d] event=LEADER_TIMEOUT", n.id)
			n.StartElection()
		}
	}
}

func (n *RM) pingReplicas() {
	n.mu.Lock()
	peers := make(map[string]pb.RMClient)
	for addr, client := range n.replicas {
		peers[addr] = client
	}
	for addr, client := range n.deadReplicas {
		peers[addr] = client
	}
	n.mu.Unlock()

	for addr, client := range peers {
		go func(addr string, client pb.RMClient) {
			ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
			defer cancel()
			_, err := client.Ping(ctx, &pb.Empty{})

			n.mu.Lock()
			defer n.mu.Unlock()
			if err != nil {
				if client, ok := n.replicas[addr]; ok {
					delete(n.replicas, addr)
					n.deadReplicas[addr] = client
					log.Printf("[RM %d] event=REPLICA_DEAD peer=%s", n.id, addr)
				}
			} else if client, ok := n.deadReplicas[addr]; ok {
				delete(n.deadReplicas, addr)
				n.replicas[addr] = client
				log.Printf("[RM %d] event=REPLICA_REVIVED peer=%s", n.id, addr)
			}

		}(addr, client)
	}
}

func (n *RM) NotifyFE() {
	if n.fe == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	_, err := n.fe.LeaderUpdate(ctx, &pb.ElectionResult{WinnerIp: n.ip, WinnerId: n.id})
	if err != nil {
		log.Printf("[RM %d] event=FE_NOTIFY_FALED err=%v", n.id, err)
	}
}

func (n *RM) AuctionTimer() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		n.mu.Lock()
		if !time.Now().Before(n.auctionEnd) {
			n.closed = true
			log.Printf("[RM %d] event=AUCTION_CLOSED winner=%s price=%d", n.id, n.highestBid.bidder, n.highestBid.bid)
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
	}
}
