use commitlog::{CommitLog, LogOptions, ReadLimit};
use commitlog::message::{MessageBuf, MessageSet};
use omnipaxos::ballot_leader_election::Ballot;
use omnipaxos::storage::{Entry, StopSign, Storage, StorageResult};
use serde::{Deserialize, Serialize};
use sled::Db;
use anyhow::{Result, bail};
use std::{iter::FromIterator, marker::PhantomData, path::PathBuf, fs::create_dir_all};
use zerocopy::{AsBytes, FromBytes};

use super::opax::OpaxRequest;

const SUBDIR_COMMITLOG: &str = "commitlog";

// REMEMBER: Moved to the slice below to block API requests to omnipaxos internal keys.
// const NPROM: &[u8] = b"NPROM";
// const ACC: &[u8] = b"ACC";
// const DECIDE: &[u8] = b"DECIDE";
// const TRIM: &[u8] = b"TRIM";
// const STOPSIGN: &[u8] = b"STOPSIGN";
// const SNAPSHOT: &[u8] = b"SNAPSHOT";

pub const OPAX_RESERVED_KEYS: [&[u8]; 6] = [
    b"NPROM",
    b"ACC",
    b"DECIDE",
    b"TRIM",
    b"STOPSIGN",
    b"SNAPSHOT"
];

// Configuration for `PersistentStorage`.
/// # Fields
/// * `path`: Path to the Commitlog and state storage
/// * `commitlog_options`: Options for the Commitlog
/// * `sled` : precreated database instance
pub struct PersistentStorageConfig {
    log_path: PathBuf,
    log_options: LogOptions,
    sled: Db,
}

impl PersistentStorageConfig {
    pub fn new(dir: &PathBuf, db: &Db) -> Result<Self> {
        let mut log_dir = dir.to_owned();
        log_dir.push(SUBDIR_COMMITLOG);

        create_dir_all(&log_dir)?;

        Ok(Self {
            log_options: LogOptions::new(&log_dir),
            log_path: log_dir,
            sled: db.to_owned(),
        })
    }
}

/// A persistent storage implementation, lets sequence paxos write the log
/// and current state to disk. Log entries are serialized and de-serialized
/// into slice of bytes when read or written from the log.
pub struct PersistentStorage<T> where T: Entry {
    commitlog: CommitLog,
    log_path: PathBuf,
    sled: Db,
    t: PhantomData<T>,
}

impl<T: Entry> PersistentStorage<T> {
    pub fn open(config: PersistentStorageConfig) -> Result<Self> {
        let commitlog = match CommitLog::new(config.log_options) {
            Ok(l) => l,
            Err(e) => bail!("Failed to create commitlog: {}", e),
        };

        Ok(Self {
            commitlog, log_path: config.log_path, sled: config.sled, t: PhantomData
        })
    }
    // pub fn sled_ref(&self) -> &Db {
    //     &self.sled
    // }
}

/// An error returning the proposal that was failed due to that the current configuration is stopped.
#[derive(Copy, Clone, Debug)]
pub struct ErrHelper {}
impl std::error::Error for ErrHelper {}
impl std::fmt::Display for ErrHelper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl<T> Storage<T> for PersistentStorage<T>
where
    T: Entry + Serialize + for<'a> Deserialize<'a>,
    T::Snapshot: Serialize + for<'a> Deserialize<'a>,
{
    fn append_entry(&mut self, entry: T) -> StorageResult<u64> {
        let entry_bytes = bincode::serialize(&entry)?;
        let offset = self.commitlog.append_msg(entry_bytes)?;
        self.commitlog.flush()?; // ensure durable writes
        Ok(offset + 1) // +1 as commitlog returns the offset the entry was appended at, while we should return the index that the entry got in the log.
    }

    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<u64> {
        // Required check because Commitlog has a bug where appending an empty set of entries will
        // always return an offset.first() with 0 despite entries being in the log.
        if entries.is_empty() {
            return self.get_log_len();
        }
        let mut serialized = vec![];
        for entry in entries {
            serialized.push(bincode::serialize(&entry)?)
        }
        let offset = self
            .commitlog
            .append(&mut MessageBuf::from_iter(serialized))?;
        self.commitlog.flush()?; // ensure durable writes
        Ok(offset.first() + offset.len() as u64)
    }

    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> StorageResult<u64> {
        if from_idx > 0 && from_idx < self.get_log_len()? {
            self.commitlog.truncate(from_idx)?;
        }
        self.append_entries(entries)
    }

    fn get_entries(&self, from: u64, to: u64) -> StorageResult<Vec<T>> {
        // Check if the commit log has entries up to the requested endpoint.
        if to > self.commitlog.next_offset() || from >= to {
            return Ok(vec![]); // Do an early return
        }

        let buffer = self.commitlog.read(from, ReadLimit::default())?;
        let mut entries = Vec::<T>::with_capacity((to - from) as usize);
        let mut iter = buffer.iter();
        for _ in from..to {
            let msg = iter.next().ok_or(ErrHelper {})?;
            entries.push(bincode::deserialize(msg.payload())?);
        }
        Ok(entries)
    }

    fn get_log_len(&self) -> StorageResult<u64> {
        Ok(self.commitlog.next_offset())
    }

    fn get_suffix(&self, from: u64) -> StorageResult<Vec<T>> {
        self.get_entries(from, self.commitlog.next_offset())
    }

    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        {
            let promised = self.sled.get(OPAX_RESERVED_KEYS[0])?;
            match promised {
                Some(prom_bytes) => {
                    let ballot = bincode::deserialize(&prom_bytes)?;
                    Ok(Some(ballot))
                }
                None => Ok(Some(Ballot::default())),
            }
        }
    }

    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        let prom_bytes = bincode::serialize(&n_prom)?;
        self.sled.insert(OPAX_RESERVED_KEYS[0], prom_bytes)?;
        Ok(())
    }

    fn get_decided_idx(&self) -> StorageResult<u64> {
        let decided = self.sled.get(OPAX_RESERVED_KEYS[2])?;
        match decided {
            Some(ld_bytes) => Ok(u64::read_from(ld_bytes.as_bytes()).ok_or(ErrHelper {})?),
            None => Ok(0),
        }
    }

    fn set_decided_idx(&mut self, ld: u64) -> StorageResult<()> {
        let ld_bytes = u64::as_bytes(&ld);
        self.sled.insert(OPAX_RESERVED_KEYS[2], ld_bytes)?;
        Ok(())
    }

    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        let accepted = self.sled.get(OPAX_RESERVED_KEYS[1])?;
        match accepted {
            Some(acc_bytes) => {
                let ballot = bincode::deserialize(&acc_bytes)?;
                Ok(Some(ballot))
            }
            None => Ok(Some(Ballot::default())),
        }
    }

    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        let acc_bytes = bincode::serialize(&na)?;
        self.sled.insert(OPAX_RESERVED_KEYS[1], acc_bytes)?;
        Ok(())
    }

    fn get_compacted_idx(&self) -> StorageResult<u64> {
        let trim = self.sled.get(OPAX_RESERVED_KEYS[3])?;
        match trim {
            Some(trim_bytes) => Ok(u64::read_from(trim_bytes.as_bytes()).ok_or(ErrHelper {})?),
            None => Ok(0),
        }
    }

    fn set_compacted_idx(&mut self, trimmed_idx: u64) -> StorageResult<()> {
        let trim_bytes = u64::as_bytes(&trimmed_idx);
        self.sled.insert(OPAX_RESERVED_KEYS[3], trim_bytes)?;
        Ok(())
    }

    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        let stopsign = self.sled.get(OPAX_RESERVED_KEYS[4])?;
        match stopsign {
            Some(ss_bytes) => Ok(bincode::deserialize(&ss_bytes)?),
            None => Ok(None),
        }
    }

    fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()> {
        let stopsign = bincode::serialize(&s)?;
        self.sled.insert(OPAX_RESERVED_KEYS[4], stopsign)?;
        Ok(())
    }

    fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        let snapshot = self.sled.get(OPAX_RESERVED_KEYS[5])?;
        if let Some(snapshot_bytes) = snapshot {
            Ok(bincode::deserialize(snapshot_bytes.as_bytes())?)
        } else {
            Ok(None)
        }
    }

    fn set_snapshot(&mut self, snapshot: Option<T::Snapshot>) -> StorageResult<()> {
        let stopsign = bincode::serialize(&snapshot)?;
        self.sled.insert(OPAX_RESERVED_KEYS[5], stopsign)?;
        Ok(())
    }

    // TODO: A way to trim the commitlog without deleting and recreating the log
    fn trim(&mut self, trimmed_idx: u64) -> StorageResult<()> {
        let trimmed_log: Vec<T> = self.get_entries(trimmed_idx, self.commitlog.next_offset())?; // get the log entries from 'trimmed_idx' to latest
        std::fs::remove_dir_all(&self.log_path)?; // remove old log
        let c_opts = LogOptions::new(&self.log_path);
        self.commitlog = CommitLog::new(c_opts)?; // create new commitlog
        self.append_entries(trimmed_log)?;
        Ok(())
    }
}

/// An in-memory storage implementation for SequencePaxos.
#[derive(Clone)]
pub struct MemoryStorage<T>
where
    T: Entry,
{
    /// Vector which contains all the logged entries in-memory.
    log: Vec<T>,
    /// Last promised round.
    n_prom: Ballot,
    /// Last accepted round.
    acc_round: Ballot,
    /// Length of the decided log.
    ld: u64,
    /// Garbage collected index.
    trimmed_idx: u64,
    /// Stored snapshot
    snapshot: Option<T::Snapshot>,
    /// Stored StopSign
    stopsign: Option<StopSign>,
}

impl<T> Storage<T> for MemoryStorage<T>
where
    T: Entry,
{
    fn append_entry(&mut self, entry: T) -> StorageResult<u64> {
        self.log.push(entry);
        self.get_log_len()
    }

    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<u64> {
        let mut e = entries;
        self.log.append(&mut e);
        self.get_log_len()
    }

    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> StorageResult<u64> {
        self.log.truncate(from_idx as usize);
        self.append_entries(entries)
    }

    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        self.n_prom = n_prom;
        Ok(())
    }

    fn set_decided_idx(&mut self, ld: u64) -> StorageResult<()> {
        self.ld = ld;
        Ok(())
    }

    fn get_decided_idx(&self) -> StorageResult<u64> {
        Ok(self.ld)
    }

    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        self.acc_round = na;
        Ok(())
    }

    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        Ok(Some(self.acc_round))
    }

    fn get_entries(&self, from: u64, to: u64) -> StorageResult<Vec<T>> {
        Ok(self
            .log
            .get(from as usize..to as usize)
            .unwrap_or(&[])
            .to_vec())
    }

    fn get_log_len(&self) -> StorageResult<u64> {
        Ok(self.log.len() as u64)
    }

    fn get_suffix(&self, from: u64) -> StorageResult<Vec<T>> {
        Ok(match self.log.get(from as usize..) {
            Some(s) => s.to_vec(),
            None => vec![],
        })
    }

    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        Ok(Some(self.n_prom))
    }

    fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()> {
        self.stopsign = s;
        Ok(())
    }

    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        Ok(self.stopsign.clone())
    }

    fn trim(&mut self, trimmed_idx: u64) -> StorageResult<()> {
        self.log
            .drain(0..(trimmed_idx as usize).min(self.log.len()));
        Ok(())
    }

    fn set_compacted_idx(&mut self, trimmed_idx: u64) -> StorageResult<()> {
        self.trimmed_idx = trimmed_idx;
        Ok(())
    }

    fn get_compacted_idx(&self) -> StorageResult<u64> {
        Ok(self.trimmed_idx)
    }

    fn set_snapshot(&mut self, snapshot: Option<T::Snapshot>) -> StorageResult<()> {
        self.snapshot = snapshot;
        Ok(())
    }

    fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        Ok(self.snapshot.clone())
    }
}

impl<T: Entry> Default for MemoryStorage<T> {
    fn default() -> Self {
        Self {
            log: vec![],
            n_prom: Ballot::default(),
            acc_round: Ballot::default(),
            ld: 0,
            trimmed_idx: 0,
            snapshot: None,
            stopsign: None,
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