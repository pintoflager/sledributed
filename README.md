# Omnipaxos distributed Sled key value database

Did this little project to wrap my head around the concept of distributed key value database in rust.

Builds on top of two rust projects and adds some http glue to play with the beast.
1. [Omnipaxos](https://github.com/haraldng/omnipaxos) replicated log
2. [Sled](https://github.com/spacejam/sled) key value database
3. [Axum](https://github.com/tokio-rs/axum) + everything http related from Tokio ecosystem

Intended to be used from linux environment, I have no idea if this builds on other OS's.
Clone this repo or download the directory and make sure you have rust toolchain installed on your machine.

Curl is normally installed on mainstream linux distros, if not that should be installed as well.

[Example](/example) directory has detailed instructions and 3-node example setup for localhost.

## How the hell

1. Open terminal and go to the project directory. Repeat 3 times.
2. On first terminal window run ```cargo run example/data_1```. Repeat changing data_1 to data_2 and so on.
3. Open one more terminal and run ```curl -X POST -H "Content-Type: application/json" -d '{"key":"first","value":"key-value pair"}' http://127.0.0.1:8080/key```

See what those 3 terminals running sledributed are saying. Should be something hinting that something was written to sled DB on each node.
