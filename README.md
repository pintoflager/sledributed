# Omnipaxos distributed Sled key value database

Did this little project to wrap my head around the concept of distributed key value database in rust.

Builds on top of two full rust projects and adds some http glue to play with the beast.
1. [Omnipaxos](https://github.com/haraldng/omnipaxos) replicated log
2. [Sled](https://github.com/spacejam/sled) key value database
3. [Axum](https://github.com/tokio-rs/axum) + everything http related from Tokio ecosystem

Intended to be used from linux environment, I have no idea if this compiles on other OS's.
Clone this repo or download the directory and make sure you have rust toolchain installed on your machine.

Curl is usually installed on mainstream linux distros, if not that should be installed as well.

## How the hell

[Example](/example) directory has detailed instructions and a 3-node example setup for localhost.

1. Open terminal and go to the project directory. Repeat 3 times.
2. On first terminal window run ```cargo run example/data_1```. Repeat hopping on to the next window and changing data_1 to data_2 and so on.
3. Open one more terminal and run ```curl -X POST -H "Content-Type: application/json" -d '{"key":"first","value":"Look a key-value pair!"}' http://127.0.0.1:8080/key```

See what those 3 terminals running sledributed are saying. Should be something suggesting that something was written to sled DB on each node.

## What it does
Binary sets up distributed key value database, creates a http server listening on peer and client requests, adds websocket client for for each peer node trying to keep connection alive and registers Omni Paxos distributed log server to keep peer nodes in sync.

- Applies (time based) caching to requests, reads first from DB and then from the memory until valid for time expires
- Prints alot on the terminal if log level is debug (default)
- Uses env variable RUST_LOG to determine logging level eg. print less: RUST_LOG=info
- Same database for omnipaxos log and key-value store, omnipaxos key access is blocked for http / websocket clients
- Requests can be made to any node on the cluster, no matter if it's read or write
- Is all async and tangled in threads
- Builds pretty fast even on my crappy laptop producing not so big (13~ mb) binary
- Has most likely errors, it would be nice to hear opinions about those