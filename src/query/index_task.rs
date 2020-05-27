use crate::cluster::{Cluster, Node};
use std::sync::Arc;
use crate::errors::{ErrorKind, Result};
use crate::{ResultCode};

#[derive(Debug, Clone)]
pub struct IndexTask {
    cluster: Arc<Cluster>,
    namespace: String,
    indexName: String
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
    pub fn new(cluster: Arc<Cluster>, namespace: String, indexName: String) -> Result<Self> {
        Ok(IndexTask {
        	cluster: cluster,
        	namespace: namespace,
        	indexName: indexName
        })
    }

    pub fn queryStatus(&self) -> Result<IndexTaskResult> {
    	let nodes = self.cluster.nodes();

    	if nodes.len() == 0 {
    		// TODO: what error should this be
    	    bail!(ErrorKind::ServerError(ResultCode::ServerError))
    	}

    	for node in nodes.iter() {
	        let command = &IndexTask::buildCommand(self.namespace.to_owned(), self.indexName.to_owned());
	        let response = node.info(
	        	Some(self.cluster.client_policy().timeout.unwrap()),
	        	&[&command[..]]
	        )?;

            // TODO: this is kind of verbose, is there an easy way to do match all 3 condition at once
	        match IndexTask::parseResponse(&response[command]) {
	        	Ok(IndexTaskResult::NotFound) => return Ok(IndexTaskResult::NotFound),
	        	Ok(IndexTaskResult::InProgress) => return Ok(IndexTaskResult::InProgress),
	            error => return error
	        }
    	}
        return Ok(IndexTaskResult::Complete);
    }

    pub fn buildCommand(namespace: String, indexName: String) -> String {
    	return format!("{}{}{}{}", "sindex/", namespace, "/", indexName);
	}

	pub fn parseResponse(response: &str) -> Result<IndexTaskResult> {
		println!("superjack response {:?}", response);
        match response.find(SUCCESS_PATTERN) {
        	None => {
        		 match (response.find(FAIL_PATTERN_201), response.find(FAIL_PATTERN_203)) {
        		 	// TODO: what error should this be?
        		 	(None, None) => bail!(ErrorKind::ServerError(ResultCode::ServerError)),
        		 	(_, _) => return Ok(IndexTaskResult::NotFound)
        		 }
        	},
        	Some(patternIndex) => {
        		let percentBegin = patternIndex + SUCCESS_PATTERN.len();

        		let percentEnd = match response[percentBegin..].find(DELMITER) {
        			// TODO: what error should this be?
        			None =>  bail!(ErrorKind::ServerError(ResultCode::ServerError)),
        			Some(percentEnd) => percentEnd
        		};
        		let percentStr = &response[percentBegin..percentBegin+percentEnd];
        		if percentStr.parse::<isize>().unwrap() != 100 {
        			return Ok(IndexTaskResult::InProgress);
        		} else {
        			return Ok(IndexTaskResult::Complete);
        		}
        	}
        }
	}
}











