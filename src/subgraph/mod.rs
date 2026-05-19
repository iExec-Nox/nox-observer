use anyhow::Result;
use graphql_client::{GraphQLQuery, reqwest::post_graphql};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Custom scalars
// The subgraph exposes two scalar types that aren't in standard GraphQL:
//   - Bytes:  hex-encoded byte string, e.g. "0xabc..."  (handle ids, addresses, tx hashes)
//   - BigInt: arbitrary-precision integer encoded as a string, e.g. "12345"
//
// We deliberately keep them as String at this layer. Stricter typing (hex
// validation, i64 for block numbers, DateTime<Utc> for timestamps, etc.) is
// the job of the mapping layer in the syncer (étape 5).
// ---------------------------------------------------------------------------
type Bytes = String;
type BigInt = String;

// ---------------------------------------------------------------------------
// graphql_client derive macros
//
// These two zero-sized structs trigger the codegen at compile time. The macro
// reads `schema.json` + `queries.graphql` and generates, for each query:
//   - a sibling module (snake_case of the struct name) — e.g. `handles_query`
//   - inside it: `Variables`, `ResponseData`, and nested structs per selection
//   - `response_derives` is applied to those generated structs
//
// These structs themselves carry no runtime state — they exist purely so the
// macro has something to attach to.
// ---------------------------------------------------------------------------
#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "generated/subgraph/schema.json",
    query_path = "src/subgraph/queries.graphql",
    response_derives = "Debug, Clone",
)]
pub struct HandlesQuery;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "generated/subgraph/schema.json",
    query_path = "src/subgraph/queries.graphql",
    response_derives = "Debug, Clone",
)]
pub struct HandleRolesQuery;

// ---------------------------------------------------------------------------
// SubgraphClient
//
// Thin wrapper around a reqwest::Client. We keep one instance per app to reuse
// the underlying HTTP connection pool (reqwest handles pooling internally).
// ---------------------------------------------------------------------------
pub struct SubgraphClient {
    http: reqwest::Client,
    url: String,
}

impl SubgraphClient {
    /// Build a client with a 30s HTTP timeout, so a hung subgraph cannot stall
    /// the polling loop indefinitely.
    pub fn new(url: String) -> Result<Self> {
        // TODO 1 — build an http client:
        //   let http = reqwest::Client::builder()
        //       .timeout(Duration::from_secs(30))
        //       .build()?;
        //
        // TODO 2 — return Self { http, url }
        todo!("build the reqwest::Client and return Self")
    }

    /// Fetch a batch of handles whose blockNumber is strictly greater than
    /// `last_block`. Results are ordered by blockNumber ascending so the caller
    /// can advance its cursor reliably.
    ///
    /// On success, returns the raw `ResponseData` generated for the query —
    /// the caller will read `.handles` from it.
    pub async fn fetch_handles(
        &self,
        last_block: BigInt,
        first: i64,
    ) -> Result<handles_query::ResponseData> {
        // TODO 1 — build the variables struct:
        //   let variables = handles_query::Variables { first, last_block };
        //   (field names match the GraphQL variable names from queries.graphql,
        //    converted to snake_case if needed)
        //
        // TODO 2 — execute the query (network call):
        //   let response = post_graphql::<HandlesQuery, _>(&self.http, &self.url, variables).await?;
        //
        // TODO 3 — handle GraphQL-level errors:
        //   - `response.errors` is an Option<Vec<graphql_client::Error>>.
        //   - If it is Some and non-empty, return an Err with the errors stringified
        //     (use anyhow::bail!("subgraph returned errors: {errs:?}")).
        //
        // TODO 4 — extract the data:
        //   - `response.data` is Option<handles_query::ResponseData>.
        //   - If None, return Err("subgraph returned no data").
        //   - Otherwise Ok(data).
        todo!("execute HandlesQuery and unwrap the response")
    }

    /// Fetch the ADMIN `HandleRole` rows for a list of handle ids.
    /// Used to derive the on-chain `caller` (the address that created each handle).
    pub async fn fetch_handle_roles(
        &self,
        handle_ids: Vec<Bytes>,
    ) -> Result<handle_roles_query::ResponseData> {
        // TODO — same pattern as fetch_handles:
        //   1) build handle_roles_query::Variables { handle_ids }
        //   2) post_graphql::<HandleRolesQuery, _>(...)
        //   3) check response.errors
        //   4) unwrap response.data
        todo!("execute HandleRolesQuery and unwrap the response")
    }
}
