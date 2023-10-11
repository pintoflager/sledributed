use omnipaxos::{ClusterConfig, ServerConfig, OmniPaxosConfig};
use anyhow::{Result, bail};
use std::collections::HashMap;
use std::path::PathBuf;
use std::env;
use tracing::info;
use serde::{Deserialize, Serialize};
use sled::{Config, Db};
use tokio::fs::{read_to_string, create_dir_all};

use crate::storage::OPAX_RESERVED_KEYS;

use super::storage::{PersistentStorageConfig, PersistentStorage};
use super::opax::{OpaxRequest, OpaxEntry};

const NODE_CONFIG_FILE_NAME: &str = "node.toml";
const SUBDIR_DATABASE: &str = "database";

#[derive(Deserialize)]
pub struct Node {
    #[serde(skip)]
    pub id: u64,
    pub addr: String,
    pub clients: Option<Vec<String>>,
    priority: Option<u32>,
    peers: Vec<String>,
}

impl Node {
    pub async fn new(dir: &PathBuf) -> Result<(OmniPaxosConfig, HashMap<u64, String>, Self)> {
        let mut file = dir.to_owned();
        file.push(NODE_CONFIG_FILE_NAME);
    
        let mut node = match file.is_file() {
            true => {
                let content = read_to_string(file).await?;
                let node = toml::from_str::<Node>(&content)?;
                node
            },
            false => {
                let mut pid = None;
                let mut nodes = vec![];
                let mut clients = vec![];
                let args = env::args().collect::<Vec<String>>();
    
                for (i, a) in args.iter().enumerate() {
                    if (a.eq("--addr") || a.eq("-a")) && args.len() >= i + 1 {
                        pid = Some(args[i + 1].to_owned());
                    }
                    else if (a.eq("--peers") || a.eq("-p")) && args.len() >= i + 1 {
                        let u64_str = args[i + 1].parse::<String>()?;
                        for i in u64_str.split(",") {
                            nodes.push(i.to_string());
                        }
                    }
                    else if (a.eq("--clients") || a.eq("-c")) && args.len() >= i + 1 {
                        for i in args[i + 1].to_owned().split(",") {
                            clients.push(i.to_string());
                        }
                    }
                }
    
                Node {
                    id: 0,
                    addr: match pid {
                        Some(u) => u,
                        None => bail!("--addr or -a flag with node index was not found"),
                    },
                    clients: Some(clients),
                    priority: None,
                    peers: match nodes.len() > 0 {
                        true => nodes,
                        false => bail!("--peers or -p flag with all cluster node indexes \
                            was not found")
                    }
                }
            }
        };
        
        // Collect all node addresses and sort the results to have indexes match
        // between the servers.
        let mut nodes = node.peers.to_owned();
        
        if ! nodes.contains(&node.addr) {
            nodes.push(node.addr.to_owned());
        }
    
        nodes.sort();
    
        // Map nodes to index - address pairs
        let map = nodes.into_iter().enumerate()
            .map(|(k, v)| ((k + 1) as u64, v))
            .collect::<HashMap<u64, String>>();
        
        // Determine our own index and update node instance.
        let pid = match map.iter().find(|(_, s)| s.eq(&&node.addr)) {
            Some((u, _)) => *u,
            None => bail!("Failed to load node index for {}", node.addr),
        };

        node.id = pid;
        
        // Build omnipaxos config
        let mut server_config = ServerConfig {
            pid,
            election_tick_timeout: 5,
            batch_size: 1,
            ..Default::default()
        };
    
        if let Some(u) = node.priority {
            server_config.leader_priority = u;
        }

        let mut nodes = map.to_owned().into_keys().collect::<Vec<u64>>();
        nodes.sort();
    
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes,
            ..Default::default()
        };

        info!("{:?}", server_config);
        info!("{:?}", cluster_config);
        
        let conf = OmniPaxosConfig { server_config, cluster_config };
        
        Ok((conf, map, node))
    }
}

pub struct Database {
    sled: Db,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DBResponse {
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overwritten_val: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl DBResponse {
    fn new<T>(key: T) -> Self where T: Into<String> {
        Self { key: key.into(), value: None, overwritten_val: None, error: None }
    }
    fn set_value<T>(&mut self, val: T) where T: Into<String> {
        self.value = Some(val.into());
    }
    fn set_overwritten_val<T>(&mut self, val: T) where T: Into<String> {
        self.overwritten_val = Some(val.into());
    }
    fn to_error<T>(&mut self, err: T) where T: Into<String> {
        self.error = Some(err.into());
    }
    fn key_denied<T>(key: T, allow_opax_keys: bool) -> Option<Self> where T: AsRef<str> {
        if ! allow_opax_keys && OPAX_RESERVED_KEYS.contains(&key.as_ref().as_bytes()) {
            let mut resp = Self::new(key.as_ref());
            resp.to_error("Restricted key, access forbidden");

            return Some(resp)
        }

        None
    }
}

impl Database {
    pub fn open(dir: &PathBuf) -> Result<Self> {
        let config = Config::new().path(dir);
        let db = match Config::open(&config) {
            Ok(d) => d,
            Err(e) => bail!("Failed to load sled DB for omnipaxos: {}", e),
        };

        Ok(Self { sled: db })
    }
    pub fn sled_ref(&self) -> &Db {
        &self.sled
    }
    pub fn action(&self, request: &OpaxRequest, allow_opax_keys: bool) -> Result<DBResponse> {
        match request {
            OpaxRequest::Put(OpaxEntry { key, value }) => {
                match DBResponse::key_denied(key, allow_opax_keys) {
                    Some(e) => Ok(e),
                    None => self.put(key, value),
                }
            }
            OpaxRequest::Delete(key) => match DBResponse::key_denied(key, allow_opax_keys) {
                Some(e) => Ok(e),
                None => self.delete(key)
            }
            OpaxRequest::Get(key) => match DBResponse::key_denied(key, allow_opax_keys) {
                Some(e) => Ok(e),
                None => self.get(key)
            },
            x => panic!("Unsupported action requested {:?}", x)
        }
    }
    fn get(&self, key: &str) -> Result<DBResponse> {
        let ivec = match self.sled.get(key.as_bytes()) {
            Ok(Some(v)) => v,
            Ok(None) => return Ok(DBResponse::new(key)),
            Err(e) => bail!("failed to get value: {}", e),
        };

        match String::from_utf8(ivec.to_vec()) {
            Ok(v) => {
                let mut resp = DBResponse::new(key);
                resp.set_value(v);

                Ok(resp)
            },
            Err(e) => bail!("Failed to read value from {} as string: {}",
                key, e),
        }
    }
    fn put(&self, key: &str, value: &str) -> Result<DBResponse> {
        match self.sled.insert(key.as_bytes(), value.as_bytes()) {
            Ok(o) => {
                let mut resp = DBResponse::new(key);
                resp.set_value(value);

                if let Some(i) = o {
                    let ow = String::from_utf8_lossy(&i);
                    resp.set_overwritten_val(ow);
                }

                Ok(resp)
            },
            Err(e) => bail!("failed to put value: {}", e),
        }
    }
    fn delete(&self, key: &str) -> Result<DBResponse> {
        match self.sled.remove(key.as_bytes()) {
            Ok(o) => {
                let mut resp = DBResponse::new(key);
                if let Some(i) = o {
                    let ow = String::from_utf8_lossy(&i);
                    resp.set_overwritten_val(ow);
                }

                Ok(resp)
            },
            Err(e) => bail!("failed to delete value: {}", e),
        }
    }
}

pub fn init_storage(dir: &PathBuf, sled: &Db) -> Result<PersistentStorage<OpaxRequest>> {
    let config  = PersistentStorageConfig::new(dir, sled)?;
    
    // extern crate commitlog:
    // LogOptions {
    //     log_dir: PathBuf,
    //     log_max_bytes: usize,
    //     index_max_bytes: usize,
    //     message_max_bytes: usize,
    // }
    // config.set_commitlog_options(commitlog_opts)


    // config.set_database_options(opts)

    Ok(PersistentStorage::open(config)?)
}

pub async fn init_database(dir: &PathBuf) -> Result<Database> {
    let mut db_path = dir.to_owned();
    db_path.push(SUBDIR_DATABASE);

    create_dir_all(&db_path).await?;

    Database::open(&db_path)
}