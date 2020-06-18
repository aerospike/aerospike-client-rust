use crate::cluster::{Cluster};
use std::sync::Arc;
use crate::task::{Status, Task};
use crate::errors::{ErrorKind, Result};
use crate::{ResultCode};
use std::time::{Duration};

/// Struct for querying udf register status
#[derive(Debug, Clone)]
pub struct RegisterTask {
    cluster: Arc<Cluster>,
    package_name: String
}

static COMMAND: &'static str = "udf-list";
static RESPONSE_PATTERN: &'static str = "filename=";


impl RegisterTask {
    // TODO: enforce access of this only to Client
    /// Initializes RegisterTask from client, creation should only be expose to Client
    pub fn new(cluster: Arc<Cluster>, package_name: String) -> Self {
        RegisterTask {
        	cluster: cluster,
            package_name: package_name
        }
    }
}


impl Task for RegisterTask {
    /// Query the status of index creation across all nodes
    fn query_status(&self) -> Result<Status> {
        let nodes = self.cluster.nodes();

        if nodes.len() == 0 {
            bail!(ErrorKind::ServerError(ResultCode::ServerError))
        }

        for node in nodes.iter() {
            let response = node.info(
                Some(self.cluster.client_policy().timeout.unwrap()),
                &[&COMMAND[..]]
            )?;

            if !response.contains_key(COMMAND) {
                return Ok(Status::NotFound);
            }

            let response_find = format!("{}{}", RESPONSE_PATTERN, self.package_name);

            match response[COMMAND].find(&response_find) {
	            None => {
	                return Ok(Status::InProgress);
	            },
	            _ => {}
            }
        }
        return Ok(Status::Complete);
    }

    fn get_timeout(&self) -> Result<Duration> {
    	match self.cluster.client_policy().timeout {
    		Some(duration) => {
    			return Ok(duration);
    		}
    		_ => {
    			bail!(ErrorKind::ServerError(ResultCode::ServerError)) 
    		}
    		
    	}
    }

}








