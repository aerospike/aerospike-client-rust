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

use crate::cluster::{Cluster, Node};
use crate::commands::buffer;
use crate::commands::as_msg_fields::AsMsgFields;
use crate::commands::{Command, SingleCommand};
use crate::errors::{Error, Result};
use crate::net::Connection;
use crate::policy::{Policy, QueryPolicy};
use crate::query::plan::IndexRangeWire;
use crate::query::QueryWhereWire;
use crate::query::plan::QueryPlan;
use crate::ResultCode;

/// Internal phase-1 of server-led query selection (field `44` WHERE + EXPLAIN).
pub(crate) struct QueryExplainCommand<'a> {
    cluster: Arc<Cluster>,
    policy: &'a QueryPolicy,
    namespace: String,
    set_name: Option<String>,
    explain_where_bytes: Vec<u8>,
    index_name_hint: Option<String>,
    task_id: u64,
    plan: Option<QueryPlan>,
}

impl<'a> QueryExplainCommand<'a> {
    pub(crate) fn new(
        cluster: Arc<Cluster>,
        policy: &'a QueryPolicy,
        namespace: String,
        set_name: Option<String>,
        explain_where_bytes: Vec<u8>,
        index_name_hint: Option<String>,
    ) -> Self {
        Self {
            cluster,
            policy,
            namespace,
            set_name,
            explain_where_bytes,
            index_name_hint,
            task_id: rand::random(),
            plan: None,
        }
    }

    pub(crate) async fn execute(mut self) -> Result<QueryPlan> {
        SingleCommand::execute(self.policy, &mut self).await?;
        self.plan
            .ok_or_else(|| Error::client_error("missing server query plan in response"))
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
            &self.policy.base_policy,
            &self.namespace,
            self.set_name.as_deref(),
            &self.explain_where_bytes,
            self.index_name_hint.as_deref(),
            self.task_id,
            self.policy.socket_timeout(),
        )
    }

    fn get_node(&mut self) -> Result<Arc<Node>> {
        self.cluster.get_random_node()
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
        true
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
        let header_length = usize::from(conn.buffer.read_u8(Some(8)));
        let result_code = ResultCode::from(conn.buffer.read_u8(Some(13)));
        let field_count = conn.buffer.read_u16(Some(26)) as usize;

        if result_code != ResultCode::Ok && result_code != ResultCode::FilteredOut {
            return Err(Error::server_error(result_code, conn.addr.clone(), None));
        }

        let header_size = buffer::MSG_TOTAL_HEADER_SIZE as usize;
        let body_size = ((sz & 0xFFFF_FFFF_FFFF) as usize).saturating_sub(header_length);
        let message_end = header_size + body_size;

        let have = conn.buffer.data_buffer.len();
        if have < message_end {
            conn.buffer.resize_buffer(message_end)?;
            conn.read_buffer_at(have, message_end - have).await?;
        }

        let fields = if field_count > 0 {
            AsMsgFields::from_buffer(
                &conn.buffer.data_buffer,
                header_size,
                field_count,
            )?
        } else {
            AsMsgFields::from_buffer(&[], 0, 0)?
        };

        self.plan = Some(QueryPlan::from_explain_response(
            result_code,
            &self.namespace,
            self.set_name.as_deref(),
            self.explain_where_bytes.clone(),
            &fields,
        )?);

        if let Some(ref plan) = self.plan {
            log_query_plan(
                &conn.addr,
                plan,
                self.index_name_hint.as_deref(),
                &self.explain_where_bytes,
            );
        }

        // Single-message response fully consumed; do not call empty_socket (it
        // re-reads the buffer as a proto header and mis-parses field TLV bytes).
        conn.buffer.data_buffer.clear();
        conn.buffer.reset_offset();
        Ok(())
    }

    fn prepare_retry(&mut self, _is_client_timeout: bool) {}
}

fn log_query_plan(
    node: &str,
    plan: &QueryPlan,
    index_name_hint: Option<&str>,
    explain_where_bytes: &[u8],
) {
    let index_hint = index_name_hint.unwrap_or("none");
    let where_flags = QueryWhereWire::flags(explain_where_bytes)
        .map(QueryWhereWire::format_policy_flags)
        .unwrap_or_else(|_| "unknown".into());
    let ael = plan.ael().unwrap_or_else(|_| "<invalid>".into());
    let set = plan.set_name().unwrap_or("");

    if plan.is_secondary_index() {
        let range = IndexRangeWire::describe_probe_range(plan.index_range_bytes().as_deref())
            .unwrap_or_else(|| "invalid".into());
        log::debug!(
            target: "query",
            "query-plan: node={node} ns={} set={set} selected sindex={} {range} indexType={:?} \
             ael={ael} indexHint={index_hint} whereFlags={where_flags}",
            plan.namespace(),
            plan.index_name().unwrap_or(""),
            plan.index_type(),
        );
        return;
    }

    log::debug!(
        target: "query",
        "query-plan: node={node} ns={} set={set} selection={:?} ael={ael} indexHint={index_hint} \
         whereFlags={where_flags}",
        plan.namespace(),
        plan.selection(),
    );
}

#[cfg(test)]
mod logging_tests {
    #[test]
    fn rust_log_query_target_respects_filter() {
        let _ = env_logger::try_init();
        log::debug!(target: "query", "query-plan: logging filter probe");
    }
}
