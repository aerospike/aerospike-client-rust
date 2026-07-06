// Copyright 2015-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::cluster::{Cluster, Node};
use crate::commands::msg_field_parser::ParsedMsgFields;
use crate::commands::{Command, SingleCommand};
use crate::errors::{Error, Result};
use crate::net::Connection;
use crate::policy::{Policy, QueryPolicy};
use crate::query::plan::{QueryPlan, QueryWhereWire};
use crate::{ResultCode, XorShift};

/// Server query explain (phase 1): field `44` WHERE with `EXPLAIN` flag.
pub struct QueryExplainCommand<'a> {
    cluster: Arc<Cluster>,
    policy: &'a QueryPolicy,
    namespace: String,
    set_name: Option<String>,
    ael: String,
    index_name_hint: Option<String>,
    task_id: u64,
    nodes: Vec<Arc<Node>>,
    node_index: AtomicUsize,
    plan: Option<QueryPlan>,
}

impl<'a> QueryExplainCommand<'a> {
    pub fn new(
        cluster: Arc<Cluster>,
        policy: &'a QueryPolicy,
        namespace: &str,
        set_name: Option<&str>,
        ael: &str,
        index_name_hint: Option<&str>,
    ) -> Result<Self> {
        if namespace.is_empty() {
            return Err(Error::InvalidArgument(
                "Query explain requires namespace".into(),
            ));
        }
        if ael.is_empty() {
            return Err(Error::InvalidArgument(
                "Query explain requires AEL WHERE clause".into(),
            ));
        }

        let nodes = cluster.nodes();
        if nodes.is_empty() {
            return Err(Error::InvalidArgument(
                "Query explain requires at least one cluster node".into(),
            ));
        }

        let mut rng = XorShift::new();
        let node_index = (rng.next_u64() as usize) % nodes.len();

        Ok(Self {
            cluster,
            policy,
            namespace: namespace.to_owned(),
            set_name: set_name.map(str::to_owned),
            ael: ael.to_owned(),
            index_name_hint: index_name_hint.map(str::to_owned),
            task_id: rng.next_u64(),
            nodes,
            node_index: AtomicUsize::new(node_index),
            plan: None,
        })
    }

    pub async fn execute(mut self) -> Result<QueryPlan> {
        SingleCommand::execute(self.policy, &mut self).await?;
        self.plan
            .ok_or_else(|| Error::ClientError("query explain returned no plan".into()))
    }
}

#[async_trait::async_trait]
impl Command for QueryExplainCommand<'_> {
    fn cluster(&self) -> Option<&Cluster> {
        Some(&self.cluster)
    }

    async fn write_timeout(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer
            .write_timeout(self.policy.base_policy.server_timeout());
        Ok(())
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await
    }

    async fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.set_query_explain(
            &self.namespace,
            self.set_name.as_deref(),
            &self.ael,
            self.index_name_hint.as_deref(),
            self.task_id,
            self.policy.socket_timeout(),
        )
    }

    fn get_node(&mut self) -> Result<Arc<Node>> {
        let idx = self.node_index.load(Ordering::Relaxed);
        Ok(self.nodes[idx].clone())
    }

    fn hint(&self) -> u8 {
        0
    }

    fn command_type(&self) -> crate::metrics::CommandType {
        crate::metrics::CommandType::Query
    }

    fn namespace(&self) -> Option<&str> {
        Some(&self.namespace)
    }

    fn can_retry(&mut self) -> bool {
        self.nodes.len() > 1
    }

    fn can_recover_connection(&mut self) -> bool {
        true
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        if let Err(err) = conn.read_header().await {
            warn!("Parse query explain result error: {err}");
            return Err(err);
        }

        conn.buffer.reset_offset();
        let sz = conn.buffer.read_u64(Some(0));
        let header_length = conn.buffer.read_u8(Some(8));
        let result_code = ResultCode::from(conn.buffer.read_u8(Some(13)));
        let field_count = conn.buffer.read_u16(Some(26)) as usize;
        let receive_size = ((sz & 0xFFFF_FFFF_FFFF) - u64::from(header_length)) as usize;

        if receive_size > 0 {
            conn.buffer.resize_buffer(receive_size)?;
            conn.read_body(receive_size).await?;
            conn.buffer.reset_offset();
        }

        if result_code != ResultCode::Ok && result_code != ResultCode::FilteredOut {
            return Err(Error::ServerError(result_code, false, conn.addr.clone()));
        }

        let fields = if field_count > 0 {
            ParsedMsgFields::from_buffer(&conn.buffer.data_buffer, 0, field_count)?
        } else {
            ParsedMsgFields::from_buffer(&[], 0, 0)?
        };

        let explain_where = QueryWhereWire::for_explain(&self.ael)?;
        self.plan = Some(QueryPlan::from_explain_response(
            result_code,
            &self.namespace,
            self.set_name.as_deref(),
            explain_where,
            &fields,
        )?);

        SingleCommand::empty_socket(conn).await
    }

    fn prepare_retry(&mut self, _is_client_timeout: bool) {
        if self.nodes.len() > 1 {
            let next = (self.node_index.load(Ordering::Relaxed) + 1) % self.nodes.len();
            self.node_index.store(next, Ordering::Relaxed);
        }
    }
}
