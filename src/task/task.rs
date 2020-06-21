
use std::time::{Duration, Instant};
use std::{thread};
use crate::errors::{ErrorKind, Result};



/// Status of task
#[derive(Debug, Clone, Copy)]
pub enum Status {
    /// long running task not found
    NotFound,
    /// long running task in progress
    InProgress,
    /// long running task completed
    Complete
}

static POLL_INTERVAL: Duration = Duration::from_secs(5);


/// Base task interface
pub trait Task {
    /// interface for query specific task status
    fn query_status(&self) -> Result<Status>;

    /// interface for retrieving client timeout
    fn get_timeout(&self) -> Result<Duration>;

    // TODO: consider adding async and only support rust 1.39.0+
    /// logic to wait until status query is completed
    fn wait_till_complete(&self) -> Result<Status> {
        let now = Instant::now();

        while now.elapsed().as_secs() < self.get_timeout()?.as_secs() {
            match self.query_status() {
                Ok(Status::NotFound) => {
                    bail!(ErrorKind::BadResponse("task status not found".to_string()))
                },
                Ok(Status::InProgress) => {
                    // do nothing and wait
                },
                error_or_complete => return error_or_complete
            }
            thread::sleep(POLL_INTERVAL);
        }

        // Timeout
        bail!(ErrorKind::Connection("query status timeout".to_string()))
    }
}












