use crate::cluster::{Cluster};
use std::sync::Arc;
use crate::errors::{ErrorKind, Result};
use crate::{ResultCode};




/// Instantiate an IndexTask instance to query Aerospike database cluster for index creation
/// status.
#[derive(Debug, Clone)]
pub struct IndexTask {
    cluster: Arc<Cluster>,
    namespace: String,
    index_name: String
}

#[derive(Debug, Clone, Copy)]
pub enum IndexTaskResult {
    NotFound,
    InProgress,
    Complete
}

static SUCCESS_PATTERN: &'static str = "load_pct=";
static FAIL_PATTERN_201: &'static str = "FAIL:201";
static FAIL_PATTERN_203: &'static str = "FAIL:203";
static DELMITER: &'static str = ";";

impl IndexTask {


    // TODO: enforce access of this only to Client
    /// Initializes IndexTask from client, creation should only be expose to Client
    pub fn new(cluster: Arc<Cluster>, namespace: String, index_name: String) -> Result<Self> {
        Ok(IndexTask {
        	cluster: cluster,
        	namespace: namespace,
            index_name: index_name
        })
    }


    /// Query the status of index creation across all nodes
    pub fn query_status(&self) -> Result<IndexTaskResult> {
    	let nodes = self.cluster.nodes();

    	if nodes.len() == 0 {
    		// TODO: what error should this be
    	    bail!(ErrorKind::ServerError(ResultCode::ServerError))
    	}

    	for node in nodes.iter() {
	        let command = &IndexTask::build_command(self.namespace.to_owned(), self.index_name.to_owned());
	        let response = node.info(
	        	Some(self.cluster.client_policy().timeout.unwrap()),
	        	&[&command[..]]
	        )?;

            // TODO: this is kind of verbose, is there an easy way to do match all 3 condition at once
	        match IndexTask::parse_response(&response[command]) {
	            Ok(IndexTaskResult::NotFound) => return Ok(IndexTaskResult::NotFound),
	            Ok(IndexTaskResult::InProgress) => return Ok(IndexTaskResult::InProgress),
	            error => return error
	        }
    	}
        return Ok(IndexTaskResult::Complete);
    }

    fn build_command(namespace: String, index_name: String) -> String {
        return format!("{}{}{}{}", "sindex/", namespace, "/", index_name);
    }

	fn parse_response(response: &str) -> Result<IndexTaskResult> {
        match response.find(SUCCESS_PATTERN) {
            None => {
                match (response.find(FAIL_PATTERN_201), response.find(FAIL_PATTERN_203)) {
                    // TODO: what error should this be?
                    (None, None) => bail!(ErrorKind::ServerError(ResultCode::ServerError)),
                    (_, _) => return Ok(IndexTaskResult::NotFound)
                }
            },
            Some(pattern_index) => {
                let percent_begin = pattern_index + SUCCESS_PATTERN.len();

                let percent_end = match response[percent_begin..].find(DELMITER) {
                    // TODO: what error should this be?
                    None =>  bail!(ErrorKind::ServerError(ResultCode::ServerError)),
                    Some(percent_end) => percent_end
                };
                let percent_str = &response[percent_begin..percent_begin+percent_end];
                if percent_str.parse::<isize>().unwrap() != 100 {
                    return Ok(IndexTaskResult::InProgress);
                } else {
                    return Ok(IndexTaskResult::Complete);
                }
            }
        }
    }
}











