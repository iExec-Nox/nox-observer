use graphql_client::{GraphQLQuery, reqwest::post_graphql};
use std::time::Duration;
use thiserror::Error;

// Subgraph custom scalars — kept as String here, parsed in the mapping layer.
pub type Bytes = String;
pub type BigInt = String;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "generated/subgraph/schema.json",
    query_path = "src/subgraph/queries.graphql",
    response_derives = "Debug, Clone"
)]
pub struct HandlesQuery;

#[derive(Debug, Error)]
pub enum SubgraphError {
    #[error("HTTP request to the subgraph failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("subgraph returned errors: {0:?}")]
    GraphqlErrors(Vec<graphql_client::Error>),

    #[error("subgraph returned no data")]
    EmptyResponse,
}

pub type SubgraphResult<T> = Result<T, SubgraphError>;

pub struct SubgraphClient {
    http: reqwest::Client,
    url: String,
}

impl SubgraphClient {
    pub fn new(url: String) -> SubgraphResult<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self { http, url })
    }

    pub async fn fetch_handles(
        &self,
        skip: i64,
        first: i64,
    ) -> SubgraphResult<handles_query::ResponseData> {
        let variables = handles_query::Variables { skip, first };
        let response = post_graphql::<HandlesQuery, _>(&self.http, &self.url, variables).await?;

        if let Some(errors) = response.errors
            && !errors.is_empty()
        {
            return Err(SubgraphError::GraphqlErrors(errors));
        }

        response.data.ok_or(SubgraphError::EmptyResponse)
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
