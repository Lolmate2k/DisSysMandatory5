package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	pb "handin5/grpc"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

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
		client:   pb.NewFEClient(conn),
		username: username,
		lamport:  0,
		bank:     bank,
	}
}

func main() {

	name := flag.String("name", "", "your username")
	bank := flag.Int64("bank", 100, "Your balance (poor)")
	feAddr := flag.String("fe", "localhost:5000", "The ip for the FE")
	flag.Parse()
	if *name == "" || *bank <= 0 {
		os.Exit(1)
	}

	logFile, err := os.OpenFile("../logs/client_"+*name+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	conn, err := grpc.NewClient(*feAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to FE: %v", err)
	}
	defer conn.Close()

	c := newClient(*name, *bank, conn)
	log.Printf("[Client %s] event=CLIENT_START fe=%s bank=%d", c.username, c.lamport, c.bank)

	go c.CommandLoop()

	select {}
}

func (c *AuctionClient) CommandLoop() {
	fmt.Printf("Hi %s, you have %d$.\n", c.username, c.bank)
	fmt.Printf("COMMANDS: bid <amount> | result | quit\n")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			return
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}

		command := fields[0]
		switch command {
		case "bid":
			if len(fields) != 2 {
				fmt.Println("Usage: bid <amount>")
				continue
			}
			amount, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil || amount <= 0 {
				fmt.Println("The amount must be a positive whole number")
				continue
			}
			c.Bid(amount)
		case "result":
			c.Result()
		case "quit":
			os.Exit(1)
			return
		default:
			fmt.Println("Unknown Command. Use the correct ones bozo")
		}
	}
}

func (c *AuctionClient) Bid(amount int64) {
	if amount > c.bank {
		fmt.Printf("You only have: %d\n", c.bank)
		return
	}

	c.lamport++
	log.Printf("[Client %s] event=BID_SEND amount=%d lamport=%d", c.username, amount, c.lamport)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	ack, err := c.client.Bid(ctx, &pb.Amount{Dollars: amount, Lamport: c.lamport, Bidder: c.username})
	cancel()

	if err == nil {
		log.Printf("[Client %s] event=BID_REPLY msg=%q lamport=%d", c.username, ack.Message, c.lamport)
		fmt.Println(ack.Message)
		return
	}

	log.Printf("[Client %s] event=BID_ERROR err=%v", c.username, err)
	fmt.Println("Your bid attempt failed, please try again")
}

func (c *AuctionClient) Result() {
	c.lamport++ 
	log.Printf("[Client %s] event=RESULT_SEND lamport=%d", c.username, c.lamport)
	
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	out, err := c.client.Result(ctx, &pb.Read{Lamport: c.lamport})
	cancel()

	if err != nil {
		log.Printf("[Client %s] event=RESULT_ERROR err=%v", c.username, err)
		return
	}

	c.lamport = max(c.lamport, out.Lamport)
	log.Printf("[Client %s] event=RESULT_REPLY winner=%s price=%d closed=%v lamport=%d", c.username, out.Winner, out.BuyingPrice, out.Closed, c.lamport)

	if out.Winner == "" {
		fmt.Println("No bids yet")
	} else if out.Closed {
		fmt.Printf("Auction is over. Winnner: %s with %d\n", out.Winner, out.BuyingPrice)
	} else {
		fmt.Printf("Auction is open. Highest bid: %d by %s\n", out.BuyingPrice, out.Winner)
	}
	
}
