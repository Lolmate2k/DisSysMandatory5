# DisSysMandatory5

The system right now supports any amount of clients (bidders), one Front End and 3 Replica Managers (RM).

**ALL COMMANDS ARE RUN FROM THE ROOT OF THE PROJECT!!!**
A Replica is started in the following way:
```bash
go run ./RM/main.go -RMid=<ID> -fe=localhost:5000
```
Make sure the ip at -fe=___ matches the ip that you start the FE with soon

The Front End is started in the following way:
```bash
go run ./FE/main.go -addr=localhost:5000
```

A Bidding Client is started in the following way:
```bash
go run ./client/main.go -name=<YOURNAME> -bank=<AnIntegerAmountOfMoney> -fe=localhost:5000
```

When everything is up and running, clients can bid against eachother.
If an auction is over, another can be started by any bidder, by submitting a bid.

The front end is seen as a static proxy, which the program in its current state does not support crashes of.