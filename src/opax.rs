use std::time::Duration;
use omnipaxos::util::LogEntry;
use omnipaxos::OmniPaxos;
use omnipaxos::messages::Message;
use omnipaxos::storage::{Entry, Snapshot};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use tokio::time;
use tracing::{debug, error};

use crate::nodes::DBResponse;

use super::client::HttpClient;
use super::nodes::Database;
use super::storage::PersistentStorage;
use super::server::HttpServer;

// Masked string type for human readers
pub type OpaxEntryKey = String;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct OpaxEntry {
    pub key: OpaxEntryKey,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum OpaxRequest {
    Put(OpaxEntry),
    Delete(OpaxEntryKey),
    Get(OpaxEntryKey),
    Ping(u64)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpaxResponse {
    Decided(u64),
    Get(DBResponse),
    Put(DBResponse),
    Delete(DBResponse),
    Pong((u64, String)),
    Blocked(OpaxRequest)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpaxMessage {
    Internal(Message<OpaxRequest>),
    APIRequest(OpaxRequest),
    APIResponse(OpaxResponse, Option<OpaxRequest>),
}

pub struct OpaxServer {
    pub omni_paxos: OmniPaxos<OpaxRequest, PersistentStorage<OpaxRequest>>,
    pub http_server: HttpServer,
    pub http_client: HttpClient,
    pub database: Database,
    pub last_decided_idx: u64,
}

impl OpaxServer {
    async fn reconnect_returned_peers(&mut self) {
        for p in self.http_client.returned_peers().await {
            debug!("Connected peer node {} to omnipaxos", p);
            self.omni_paxos.reconnected(p);
        }
    }
    async fn process_incoming_msgs(&mut self) {
        for message in self.http_server.get_received().await {
            match message {
                OpaxMessage::APIRequest(r) => self.api_request(r).await,
                OpaxMessage::Internal(m) => {
                    self.omni_paxos.handle_incoming(m);
                },
                OpaxMessage::APIResponse(r, _) => {
                    error!("Received an API response on the inbox: {:?}", r);
                }
            }
        }
    }
    /// Request to API that responds by writing back to the client socket when message
    /// comes up on outgoing message queue. 
    async fn api_request(&mut self, req: OpaxRequest) {
        match req {
            OpaxRequest::Get(ref k) => {
                let d = match self.database.action(&OpaxRequest::Get(k.clone()), false) {
                    Ok(d) => d,
                    Err(e) => panic!("Database get action failed: {}", e),
                };
                let response = OpaxResponse::Get(d);
                let message = OpaxMessage::APIResponse(response, Some(req));
                
                self.http_server.receiver(message).await;
            },
            OpaxRequest::Put(ref x) => {
                let req = OpaxRequest::Put(x.to_owned());

                if let Err(e) = self.omni_paxos.append(req.to_owned()) {
                    error!("Failed to append PUT request to replicated log: {:?}", e);
                }

                if ! self.http_client.quorum().await {
                    let response = OpaxResponse::Blocked(req.to_owned());
                    let message = OpaxMessage::APIResponse(response, Some(req));
                    
                    self.http_server.receiver(message).await;
                }
            }
            OpaxRequest::Delete(ref x) => {
                let entry = OpaxRequest::Delete(x.to_owned());

                if let Err(e) = self.omni_paxos.append(entry) {
                    error!("Failed to append DELETE request to replicated log: {:?}", e);
                }

                if ! self.http_client.quorum().await {
                    let response = OpaxResponse::Blocked(req.to_owned());
                    let message = OpaxMessage::APIResponse(response, Some(req));
                    
                    self.http_server.receiver(message).await;
                }
            },
            OpaxRequest::Ping(p) => {
                let r = self.http_client.ping_peer(self.http_server.node_id, p).await;
                let response = OpaxResponse::Pong((p, r));
                let message = OpaxMessage::APIResponse(response, Some(req.to_owned()));
                
                self.http_server.receiver(message).await;
            }
        }
    }
    async fn send_outgoing_msgs(&mut self) {
        let messages = self.omni_paxos.outgoing_messages();
        let mut receivers = self.http_server.peers.keys()
            .map(|u| *u)
            .collect::<Vec<u64>>();

        // Apparently omnipaxos sends messages to itself (instance on the current node)
        // through (normally invalid node) index 0.
        receivers.push(0);

        for msg in messages {
            let receiver = msg.get_receiver();
            let message = OpaxMessage::Internal(msg);

            if ! receivers.contains(&receiver) && self.http_server.node_id != receiver {
                error!("Sending omnipaxos message to unknown receiver {}", receiver);
            }

            if receiver == 0 {
                debug!("Omnipaxos message to receiver index 0: {:?}", message);
            }

            // If we want to send something to ourselves use server receiver method.
            match self.http_server.node_id == receiver || receiver == 0 {
                true => self.http_server.receiver(message).await,
                false => self.http_client.send(receiver, message).await,
            }
        }
    }
    async fn handle_decided_entries(&mut self) {
        let new_decided_idx = self.omni_paxos.get_decided_idx();

        if self.last_decided_idx < new_decided_idx {
            let decided_entries = self.omni_paxos.read_decided_suffix(self.last_decided_idx).unwrap();
            self.update_database(decided_entries).await;
            self.last_decided_idx = new_decided_idx;
            
            // snapshotting
            if new_decided_idx % 5 == 0 {
                debug!("Log before: {:?}", self.omni_paxos.read_decided_suffix(0).unwrap());
                
                self.omni_paxos.snapshot(Some(new_decided_idx), true)
                    .expect("Failed to snapshot");
                
                debug!("Log after: {:?}\n", self.omni_paxos.read_decided_suffix(0).unwrap());
            }
        }
    }

    async fn update_database(&self, decided_entries: Vec<LogEntry<OpaxRequest>>) {
        for entry in decided_entries {
            match entry {
                LogEntry::Decided(r) => {
                    debug!("Updating decided log entry to DB: {:?}", r);

                    let d = match self.database.action(&r, false) {
                        Ok(d) => d,
                        Err(e) => panic!("Database action failed: {}", e),
                    };
                    
                    // Update query response if present
                    let response = match r {
                        OpaxRequest::Get(_) => OpaxResponse::Get(d),
                        OpaxRequest::Delete(_) => OpaxResponse::Delete(d),
                        OpaxRequest::Put(_) => OpaxResponse::Put(d),
                        OpaxRequest::Ping(_) => panic!("Stupid developer issue: don't log pings"),
                    };

                    let message = OpaxMessage::APIResponse(response, Some(r));
                    self.http_server.receiver(message).await;
                },
                LogEntry::Undecided(r) => {
                    debug!("Ignoring database update from undecided log entry: {:?}", r);
                },
                LogEntry::Trimmed(u) => {
                    debug!("Log entry has been trimmed to index {}", u);
                },
                LogEntry::Snapshotted(s) => {
                    debug!("Updating from snapshotted log entry to DB: {:?}", s);

                    for d in s.snapshot.deleted_keys {
                        let req = OpaxRequest::Delete(d);
                        let d = match self.database.action(&req, false) {
                            Ok(r) => r,
                            Err(e) => {
                                error!("Database action failed: {}", e);
                                continue;
                            },
                        };

                        debug!("Deleted key: {} which had value: {:?}", d.key, d.overwritten_val);
                    }

                    for (k, v) in s.snapshot.snapshotted {
                        let entry = OpaxEntry { key: k, value: v};
                        let req = OpaxRequest::Put(entry);

                        let d = match self.database.action(&req, false) {
                            Ok(r) => r,
                            Err(e) => {
                                error!("Database action failed: {}", e);
                                panic!("Unable to proceed, inconsistent replication.");
                            },
                        };

                        debug!("Added key: {} with val: {}, overwriting value: {:?}",
                            d.key, d.value.unwrap(), d.overwritten_val);
                    }
                },
                LogEntry::StopSign(s, b) => {
                    debug!("Stopsign {:?} found from log entries. Decided \
                        reconfiguration: {}", s, b);
                },
            }
        }
    }

    pub async fn run(&mut self) {
        let mut msg_interval = time::interval(Duration::from_millis(1));
        let mut tick_interval = time::interval(Duration::from_millis(10));
        
        loop {
            tokio::select! {
                biased;
                _ = msg_interval.tick() => {
                    self.reconnect_returned_peers().await;
                    self.process_incoming_msgs().await;
                    self.send_outgoing_msgs().await;
                    self.handle_decided_entries().await;
                },
                _ = tick_interval.tick() => {
                    self.omni_paxos.tick();
                },
                else => (),
            }
        }
    }
}

impl Entry for OpaxRequest {
    type Snapshot = KVSnapshot;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KVSnapshot {
    snapshotted: HashMap<OpaxEntryKey, String>,
    deleted_keys: Vec<OpaxEntryKey>,
}

impl Snapshot<OpaxRequest> for KVSnapshot {
    fn create(entries: &[OpaxRequest]) -> Self {
        let mut snapshotted = HashMap::new();
        let mut deleted_keys: Vec<OpaxEntryKey> = Vec::new();
        for e in entries {
            match e {
                OpaxRequest::Put(OpaxEntry { key, value }) => {
                    snapshotted.insert(key.clone(), value.clone());
                }
                OpaxRequest::Delete(key) => {
                    if snapshotted.remove(key).is_none() {
                        // key was not in the snapshot
                        deleted_keys.push(key.clone());
                    }
                }
                OpaxRequest::Get(_) => (),
                OpaxRequest::Ping(_) => (),
            }
        }
        // remove keys that were put back
        deleted_keys.retain(|k| !snapshotted.contains_key(k));
        Self {
            snapshotted,
            deleted_keys,
        }
    }

    fn merge(&mut self, delta: Self) {
        for (k, v) in delta.snapshotted {
            self.snapshotted.insert(k, v);
        }
        for k in delta.deleted_keys {
            self.snapshotted.remove(&k);
        }
        self.deleted_keys.clear();
    }

    fn use_snapshots() -> bool {
        true
    }
}
