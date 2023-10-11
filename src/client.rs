use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{WebSocketStream, MaybeTlsStream, connect_async};
use tokio_tungstenite::tungstenite::{Error as WsError, protocol::Message};
use std::collections::HashMap;
use std::sync::Arc;
use std::io::ErrorKind as IoErr;
use tokio::net::TcpStream;
use anyhow::Result;
use tracing::{debug, info, error};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

use super::opax::OpaxMessage;

const PEER_RECONNECT_RETRY_INTERVAL_SECS: u64 = 2;

pub struct ClientSockets {
    pub write: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    pub read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

pub struct HttpClient {
    sockets: Arc<Mutex<HashMap<u64, (ClientSockets, bool)>>>,
    disconnected: Arc<Mutex<HashMap<u64, usize>>>,
    peers: HashMap<u64, String>,
}

impl HttpClient {
    pub async fn new(peers: &HashMap<u64, String>) -> Result<Self> {
        let sockets = Arc::new(Mutex::new(HashMap::new()));
        let connecting = Arc::new(Mutex::new(
            peers.keys().map(|u|(*u, 0)).collect::<HashMap<u64, usize>>()
        ));

        // Create client and connect it to peer nodes.
        let client = Self { sockets, disconnected: connecting, peers: peers.to_owned() };
        client.connect();

        Ok(client)
    }
    pub async fn returned_peers(&mut self) -> Vec<u64> {
        let mut sockets = self.sockets.lock().await;

        let returned = sockets.iter()
            .filter(|(_, (_, b))|*b)
            .map(|(u,_)| *u)
            .collect::<Vec<u64>>();
        
        if returned.is_empty() {
            return vec![]
        }

        for (_, (_, b)) in sockets.iter_mut() {
            *b = false;
        }

        returned
    }
    // I think this is how this works. Majority of nodes has to communicate in order for
    // them to decide on writes. For 3 node cluster this is 2 and 5 node cluster it's 3.
    pub async fn quorum(&mut self) -> bool {
        let total = self.peers.len();
        let connected = self.sockets.lock().await.len();
        let majority = ((total / 2) as f32).ceil();
        
        connected as f32 >= majority
    }
    pub async fn send(&mut self, receiver: u64, message: OpaxMessage) {
        let mut sockets = self.sockets.lock().await;

        if let Some((c, _)) = sockets.get_mut(&receiver) {
            let message = match serde_json::to_vec(&message) {
                Ok(s) => Message::Binary(s),
                Err(e) => {
                    error!("Failed to read json message to bytes: {}", e);
                    return
                },
            };

            if let Err(e) = c.write.send(message).await {
                match e {
                    WsError::Io(io_e) => match io_e.kind() {
                        IoErr::ConnectionRefused => (), // peer not connectable
                        IoErr::BrokenPipe => (), // peer connection dropped
                        _ => error!("Failed to send message to node {}: {}", receiver, io_e),
                    },
                    _ => error!("Peer node {} connection issue: {}", receiver, e),
                }

                // Drop disconnected socket from the list
                if sockets.remove(&receiver).is_none() {
                    panic!("Failed to remove dropped connection to peer node {}",
                        receiver);
                }

                // Add disconnected peer to reconnecting queue.
                self.disconnected.lock().await.insert(receiver, 0);
            }
        }
        // TODO: debugginh
        // else {
        //     error!("No open socket to receiver peer {}. Failed to send message {:?}",
        //         receiver, message);
        // }
    }
    pub fn connect(&self) {
        let connecting = self.disconnected.clone();
        let sockets = self.sockets.clone();
        let peers = self.peers.to_owned();

        tokio::spawn(async move {
            loop {
                let conns = connecting.lock().await;
                
                // New or disconnected peer nodes appear on this list.
                for (p, x) in conns.iter() {
                    if *x == 0 {
                        debug!("Trying to establish connection to peer node {}...", p);
                    }
                    
                    let addr = match peers.get(p) {
                        Some(a) => format!("ws://{}/omnipax", a),
                        None => panic!("Unable to find peer node {} from client \
                            peers list", p),
                    };

                    connect_socket(*p, *x, addr, &sockets, &connecting);
                }

                // Sleep for awhile to let the spawned tasks to do their thing.
                sleep(Duration::from_secs(PEER_RECONNECT_RETRY_INTERVAL_SECS)).await;
            }
        });
    }
    pub async fn ping_peer(&self, node: u64, peer: u64) -> String {
        let mut conns = self.sockets.lock().await;

        if let Some((c, _)) = conns.get_mut(&peer) {
            let message = format!("Node {}", node);
            let item = Message::Ping(message.into_bytes());

            if c.write.send(item).await.is_ok() {
                return format!("Pinged peer node {} successfully", peer)
            }
        }

        format!("Failed to ping peer node {}. Peer unreachable.", peer)
    }
}

fn connect_socket(peer: u64, fails: usize, addr: String, sockets: &Arc<Mutex<HashMap<u64, (ClientSockets, bool)>>>,
connections: &Arc<Mutex<HashMap<u64, usize>>>) {
    let sockets = sockets.clone();
    let conns = connections.clone();

    tokio::spawn(async move {
        let ws_stream = match connect_async(addr).await {
            Ok(t) => t.0,
            Err(ws_e) => {
                // Known error cases for silent ingnoring
                match ws_e {
                    WsError::Io(io_e) => match io_e.kind() {
                        IoErr::ConnectionRefused => (), // peer not connectable
                        IoErr::BrokenPipe => (), // peer connection dropped
                        _ => error!("Peer node {} stream issue: {}", peer, io_e),
                    },
                    _ => error!("Peer node {} connection issue: {}", peer, ws_e),
                }

                if fails % 10 == 0 {
                    debug!("Keep trying to connect peer node {}...", peer);
                }
                
                let mut retry = conns.lock().await;
                retry.insert(peer, fails + 1);

                return
            }
        };
        
        // Add new socket to opens sockets and remove peer from
        // reconnecting queue.
        let t = ws_stream.split();
        sockets.lock().await.insert(
            peer,
            (ClientSockets { read: t.1, write: t.0 }, true)
        );
        
        info!("Client connected to peer node {}!", peer);
        
        let mut retry = conns.lock().await;
        if retry.remove(&peer).is_none() {
            panic!("Unable to remove connection retrier for peer node {}", peer)
        }
    });
}
