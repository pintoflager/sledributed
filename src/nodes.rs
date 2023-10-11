use omnipaxos::{ClusterConfig, ServerConfig, OmniPaxosConfig};
use anyhow::{Result, bail};
use std::collections::HashMap;
use std::{path::PathBuf, time::Duration};
use std::env;
use tracing::info;
use serde::{Deserialize, Serialize};
use sled::{Config, Db};
use tokio::fs::{read_to_string, create_dir_all};

use super::storage::OPAX_RESERVED_KEYS;
use super::opax::{OpaxRequest, OpaxEntry};

const NODE_CONFIG_FILE_NAME: &str = "node.toml";
const SUBDIR_DATABASE: &str = "database";

enum CacheTime {
    S(u64),
    M(u64),
    H(u64),
    D(u64)
}

impl CacheTime {
    fn from_str(val: &str, amount: u64) -> Self {
        match val.to_lowercase().as_str() {
            "s" => Self::S(amount),
            "m" => Self::M(amount),
            "h" => Self::H(amount),
            "d" => Self::D(amount),
            x => panic!("Unknown time unit '{}' given", x),
        }
    }
    fn as_duration(&self) -> Duration {
        match self {
            Self::S(u) => Duration::from_secs(*u),
            Self::M(u) => Duration::from_secs(*u * 60),
            Self::H(u) => Duration::from_secs(*u * 60 * 60),
            Self::D(u) => Duration::from_secs(*u * 60 * 60 * 24),
        }
    }
}

#[derive(Deserialize)]
pub struct ClusterNode {
    #[serde(skip)]
    pub id: u64,
    #[serde(skip)]
    pub cache: Duration,
    pub addr: String,
    pub clients: Option<Vec<String>>,
    pub cache_time: Option<String>,
    priority: Option<u32>,
    peers: Vec<String>,
}

impl ClusterNode {
    pub async fn new(dir: &PathBuf) -> Result<(OmniPaxosConfig, HashMap<u64, String>, Self)> {
        let mut file = dir.to_owned();
        file.push(NODE_CONFIG_FILE_NAME);
    
        let mut node = match file.is_file() {
            true => {
                let content = read_to_string(file).await?;
                let node = toml::from_str::<ClusterNode>(&content)?;
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
    
                Self {
                    id: 0,
                    cache: Duration::from_secs(0),
                    cache_time: None,
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

        // Determine cache lifetime
        node.cache = match node.cache_time {
            Some(ref s) => match s.len() >= 2 {
                true => {
                    let (a, u) = s.split_at(s.len() - 1);
                    let amount = match a.parse::<u64>() {
                        Ok(u) => u,
                        Err(e) => panic!("Failed to read time unit as \
                            number: {}", e),
                    };

                    CacheTime::from_str(u, amount).as_duration()
                },
                false => panic!("Cache time should be a integer and time unit s,m,h or d"),
            },
            None => Duration::from_secs(60 * 60 * 5), // 5 hours
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

pub struct NodeDatabase {
    sled: Db,
}

impl NodeDatabase {
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
    pub fn action(&self, request: &OpaxRequest, allow_opax_keys: bool) -> Result<DBQueryResult> {
        match request {
            OpaxRequest::Put(OpaxEntry { key, value }) => {
                match DBQueryResult::key_denied(key, allow_opax_keys) {
                    Some(e) => Ok(e),
                    None => self.put(key, value),
                }
            }
            OpaxRequest::Delete(key) => match DBQueryResult::key_denied(key, allow_opax_keys) {
                Some(e) => Ok(e),
                None => self.delete(key)
            }
            OpaxRequest::Get(key) => match DBQueryResult::key_denied(key, allow_opax_keys) {
                Some(e) => Ok(e),
                None => self.get(key)
            },
            x => panic!("Unsupported action requested {:?}", x)
        }
    }
    fn get(&self, key: &str) -> Result<DBQueryResult> {
        let ivec = match self.sled.get(key.as_bytes()) {
            Ok(Some(v)) => v,
            Ok(None) => return Ok(DBQueryResult::new(key)),
            Err(e) => bail!("failed to get value: {}", e),
        };

        match String::from_utf8(ivec.to_vec()) {
            Ok(v) => {
                let mut resp = DBQueryResult::new(key);
                resp.set_value(v);

                Ok(resp)
            },
            Err(e) => bail!("Failed to read value from {} as string: {}",
                key, e),
        }
    }
    fn put(&self, key: &str, value: &str) -> Result<DBQueryResult> {
        match self.sled.insert(key.as_bytes(), value.as_bytes()) {
            Ok(o) => {
                let mut resp = DBQueryResult::new(key);
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
    fn delete(&self, key: &str) -> Result<DBQueryResult> {
        match self.sled.remove(key.as_bytes()) {
            Ok(o) => {
                let mut resp = DBQueryResult::new(key);
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

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct DBQueryResult {
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overwritten_val: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl DBQueryResult {
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

pub async fn init_database(dir: &PathBuf) -> Result<NodeDatabase> {
    let mut db_path = dir.to_owned();
    db_path.push(SUBDIR_DATABASE);

    create_dir_all(&db_path).await?;

    NodeDatabase::open(&db_path)
}