use core::time;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use futures::executor::block_on;
use futures::future::ok;
use futures::lock::Mutex;
use std::collections::HashMap;

use labrpc::*;
use labrpc::Error;

use crate::service::{TSOClient, TransactionClient, timestamp};
use crate::msg::{TimestampRequest, TimestampResponse, GetRequest, GetResponse};
use crate::msg::{PrewriteRequest, PrewriteResponse, CommitRequest, CommitResponse};

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,
    deltas: HashMap<Vec<u8>, Vec<u8>>,
    start_ts: u64
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {
            tso_client: tso_client, 
            txn_client: txn_client, 
            deltas: HashMap::new(), 
            start_ts: 0
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        let mut back_off_time = BACKOFF_TIME_MS;
        for retry in (0..RETRY_TIMES) {
            let res = block_on(
                self.tso_client.get_timestamp(&TimestampRequest {})
            );
            if let Ok(t) = res {
                return Ok(t.ts);
            }
            sleep(Duration::from_millis(back_off_time));
            back_off_time *= 2;
        }
        Err(Error::Timeout)
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        if let Ok(timestamp) = self.get_timestamp() {
            self.start_ts = timestamp;
        } else {
            panic!("Cannot get start_ts.");
        }
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        let get_request = GetRequest {
            ts: self.start_ts,
            key: key
        };

        for retry in (0..RETRY_TIMES) {
            let res = block_on(
                self.txn_client.get(&get_request)
            );
            if let Ok(resp) = res {
                return Ok(resp.value);
            }
            sleep(Duration::from_millis(
               (1 << retry) * BACKOFF_TIME_MS
            ));
        }
        Err(Error::Timeout)
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        self.deltas.insert(key, value);
    }

    fn prewrite(&self, req: PrewriteRequest) -> Result<bool> {
        for retry in (0..RETRY_TIMES) {
            if let Ok(resp) = block_on(self.txn_client.prewrite(&req)) {
                return Ok(resp.okay);
            }

            sleep(Duration::from_millis(BACKOFF_TIME_MS * (1 << retry)));
        }
        Err(labrpc::Error::Timeout)
    }

    fn inner_commit(&self, is_primary: bool, commit_ts: u64, key: &Vec<u8>) -> Result<bool> {
        let req = CommitRequest {
            is_primary: is_primary,
            start_ts: self.start_ts,
            commit_ts: commit_ts, 
            key: key.clone()
        };

        for retry in (0..RETRY_TIMES) {
            if let Ok(resp) = block_on(self.txn_client.commit(&req)) {
                return Ok(resp.okay);
            }

            sleep(Duration::from_millis(BACKOFF_TIME_MS * (1 << retry)));
        }
        Err(labrpc::Error::Timeout)
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // Your code here.
        let len = self.deltas.len();
        if len == 0 {
            return Ok(true);
        }

        let mut delta_iter = self.deltas.iter();
        let primary = delta_iter.next().unwrap();
        let secondaries = delta_iter.collect::<Vec<_>>();
        
        // 2PC: first phase
        let mut delta_iter = self.deltas.iter();
        while let Some((key, value)) = delta_iter.next() {
            let pw_args = PrewriteRequest{
                start_ts: self.start_ts,
                key: key.clone(),
                value: value.clone(),
                primary_key: primary.0.clone(),
            };
            let okay = self.prewrite(pw_args)?;
            if !okay {
                return Ok(false);
            }
        }

        let commit_ts = self.get_timestamp()?;
        
        // 2PC: second phase
        let commit_primary = self.inner_commit(true, commit_ts, primary.0)?;
        if !commit_primary {
            return Ok(false);
        }
        for (key, _) in secondaries {
            self.inner_commit(false, commit_ts, key);
        }

        Ok(true)
    }
}
