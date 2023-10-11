mod nodes;
mod opax;
mod storage;
mod server;
mod client;

use std::{env, path::PathBuf, collections::HashMap};
use tokio;
use tokio::fs::create_dir_all;
use tracing::{debug, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use nodes::{ClusterNode, init_database};
use storage::init_storage;
use opax::OpaxServer;
use server::HttpServer;
use client::HttpClient;

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                // axum logs rejections from built-in extractors with the `axum::rejection`
                // target, at `TRACE` level. `axum::rejection=trace` enables showing those events
                "sledributed=debug,tower_http=debug,axum::rejection=trace".into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let pwd = env::current_dir().expect("Don't know what's the present working dir?");
    let dir = match env::args().nth(1) {
        Some(a) => match a.starts_with("-") {
            true => pwd,
            false => PathBuf::from(a),
        },
        None => pwd,
    };

    debug!("Parent directory for data {:?}", &dir);

    create_dir_all(&dir).await.expect(&format!("Failed to create data dir {:?}", &dir));

    let (config, map, node) = match ClusterNode::new(&dir).await {
        Ok(c) => c,
        Err(e) => panic!("Unable to load node config: {}", e),
    };

    let database = match init_database(&dir).await {
        Ok(d) => d,
        Err(e) => panic!("Failed to init DB: {}", e),
    };

    debug!("Initialized sled database for node {}", node.id);

    let storage = init_storage(&dir, database.sled_ref())
        .expect("Persistent storage failed to load");

    debug!("Initialized persistent storage for onmipaxos log");

    let omni_paxos = config
        .build(storage)
        .expect("failed to build OmniPaxos server node");

    let peers = map.into_iter()
        .filter(|(i, _)|i.ne(&node.id))
        .collect::<HashMap<u64, String>>();

    info!("Running listener for node {} on {}...", node.id, node.addr);

    let http_server = match HttpServer::new(node, &peers).await {
        Ok(s) => s,
        Err(e) => panic!("Failed to run listener: {}", e),
    };

    debug!("Preparing http client for node connections...");

    // Client connects to all peers.
    let http_client = match HttpClient::new(&peers).await {
        Ok(c) => c,
        Err(e) => panic!("Failed to set up http client for websocket \
            connections: {}", e)
    };
    
    debug!("Preparing omnipaxos server instance...");

    let mut server = OpaxServer {
        last_decided_idx: omni_paxos.get_decided_idx(),
        omni_paxos,
        http_server,
        http_client,
        database,
    };

    info!("Running omnipaxos server...");

    server.run().await;
}
