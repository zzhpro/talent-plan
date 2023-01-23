use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{clone, vec};

use futures::lock::{self, MutexGuard};

use crate::msg::*;
use crate::service::*;
use crate::*;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
    last_ret: Arc<Mutex<u64>>,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        // Your code here.
        let mut last_ret = self.last_ret.lock().unwrap();
        let mut cur_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        if cur_time <= *last_ret {
            cur_time = *last_ret + 1;
        }
        *last_ret = cur_time;
        Ok(TimestampResponse { ts: cur_time })
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.
        let ts_start_inclusive = ts_start_inclusive.unwrap_or(0);
        let ts_end_inclusive = ts_end_inclusive.unwrap_or(u64::MAX);

        let map = match column {
            Column::Data => &self.data,
            Column::Lock => &self.lock,
            Column::Write => &self.write,
        };

        map.range((
            Included((key.clone(), ts_start_inclusive)),
            Included((key, ts_end_inclusive)),
        ))
        .last()
    }

    fn read_first(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.
        let ts_start_inclusive = ts_start_inclusive.unwrap_or(0);
        let ts_end_inclusive = ts_end_inclusive.unwrap_or(u64::MAX);

        let map = match column {
            Column::Data => &self.data,
            Column::Lock => &self.lock,
            Column::Write => &self.write,
        };

        map.range((
            Included((key.clone(), ts_start_inclusive)),
            Included((key, ts_end_inclusive)),
        ))
        .next()
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        let map = match column {
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
            Column::Write => &mut self.write,
        };

        map.insert((key, ts), value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        // Your code here.
        let map = match column {
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
            Column::Write => &mut self.write,
        };

        map.remove(&(key, commit_ts));
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        // Your code here.
        let start_ts = req.ts;
        let key = req.key;

        loop {
            let mut kv_table = self.data.lock().unwrap();
            if let Some(_) = kv_table.read(key.clone(), Column::Lock, None, Some(start_ts)) {
                drop(kv_table);
                self.back_off_maybe_clean_up_lock(start_ts, key.clone());
                continue;
            }

            if let Some((_, Value::Timestamp(write_ts))) =
                kv_table.read(key.clone(), Column::Write, None, Some(start_ts))
            {
                println!("Read data at {}", write_ts);
                if let Some((_, Value::Vector(v))) =
                    kv_table.read(key.clone(), Column::Data, Some(*write_ts), Some(*write_ts))
                {
                    return Ok(GetResponse {
                        okay: true,
                        value: v.clone(),
                    });
                }
            }
            return Ok(GetResponse {
                okay: false,
                value: vec![],
            });
        }
        Err(labrpc::Error::Stopped)
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        // Your code here.
        let PrewriteRequest {
            start_ts,
            key,
            value,
            primary_key,
        } = req;
        let mut kv_table = self.data.lock().unwrap();

        if let Some(_) = kv_table.read(key.clone(), Column::Write, Some(start_ts), None) {
            return Ok(PrewriteResponse { okay: false });
        }

        if let Some(_) = kv_table.read(key.clone(), Column::Lock, None, None) {
            return Ok(PrewriteResponse { okay: false });
        }

        kv_table.write(
            key.clone(),
            Column::Lock,
            start_ts,
            Value::Vector(primary_key),
        );
        kv_table.write(key, Column::Data, start_ts, Value::Vector(value));

        Ok(PrewriteResponse { okay: true })
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        // Your code here.
        let CommitRequest {
            is_primary,
            start_ts,
            commit_ts,
            key,
        } = req;
        let mut kv_table = self.data.lock().unwrap();
        if is_primary {
            if let None = kv_table.read(key.clone(), Column::Lock, Some(start_ts), Some(start_ts)) {
                return Ok(CommitResponse { okay: false });
            }
        }

        kv_table.write(
            key.clone(),
            Column::Write,
            commit_ts,
            Value::Timestamp(start_ts),
        );
        kv_table.erase(key.clone(), Column::Lock, start_ts);

        println!("Committed {:?} at {}", key, commit_ts);
        Ok(CommitResponse { okay: true })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        // Your code here.
        sleep(Duration::from_nanos(TTL));
        // The lock has exprired, determine rolling backward or forward
        let mut kv_table = &mut self.data.lock().unwrap();
        if let Some(((_, pts), Value::Vector(pkey))) =
            kv_table.read(key.clone(), Column::Lock, None, Some(start_ts))
        {
            let primary_ts = pts.clone();
            let primary_key = pkey.clone();
            if let Some(primary_lock) = kv_table.read(primary_key.clone(), Column::Lock, 
                Some(primary_ts), Some(primary_ts)) {
                println!("Backward: lock at {:?} {}", primary_key.clone(), primary_ts);
                kv_table.erase(primary_key.clone(), Column::Lock, primary_ts);
                kv_table.erase(key, Column::Lock, primary_ts);
            } else {
                println!("Forward {:?} {:?}", key, primary_key);
                // Primary lock is gone, we must roll forward
                if let Some(((_, commit_ts), Value::Timestamp(data_ts))) = kv_table.read_first(primary_key, Column::Write, Some(primary_ts), None) {
                    let cts = *commit_ts;
                    let dts = *data_ts;
                    kv_table.write(
                        key.clone(),
                        Column::Write,
                        cts,
                        Value::Timestamp(dts),
                    );
                    kv_table.erase(key, Column::Lock, primary_ts);   
                } else {
                    kv_table.erase(key, Column::Lock, primary_ts);
                }
            }
        }
    }
}
