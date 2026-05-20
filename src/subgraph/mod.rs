use anyhow::{Result, anyhow, bail};
use graphql_client::{GraphQLQuery, reqwest::post_graphql};
use std::time::Duration;

// Subgraph custom scalars — kept as String here, parsed in the mapping layer.
type Bytes = String;
type BigInt = String;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "generated/subgraph/schema.json",
    query_path = "src/subgraph/queries.graphql",
    response_derives = "Debug, Clone"
)]
pub struct HandlesQuery;

pub struct SubgraphClient {
    http: reqwest::Client,
    url: String,
}

impl SubgraphClient {
    pub fn new(url: String) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self { http, url })
    }

    pub async fn fetch_handles(
        &self,
        last_block: BigInt,
        first: i64,
    ) -> Result<handles_query::ResponseData> {
        let variables = handles_query::Variables { first, last_block };
        let response = post_graphql::<HandlesQuery, _>(&self.http, &self.url, variables).await?;

        if let Some(errors) = response.errors
            && !errors.is_empty()
        {
            bail!("subgraph returned errors: {errors:?}");
        }

        response
            .data
            .ok_or_else(|| anyhow!("subgraph returned no data for HandlesQuery"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_builds_client_with_timeout() {
        let client =
            SubgraphClient::new("https://example.com".to_string()).expect("should build client");
        assert_eq!(client.url, "https://example.com");
    }
}
