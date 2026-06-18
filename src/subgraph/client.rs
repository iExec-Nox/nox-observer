use graphql_client::{GraphQLQuery, reqwest::post_graphql};
use std::time::Duration;

use crate::errors::{SubgraphError, SubgraphResult};

pub type Bytes = String;
pub type BigInt = String;

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
    pub fn new(url: String) -> SubgraphResult<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self { http, url })
    }

    pub async fn fetch_handles(
        &self,
        cursor_block: i64,
        first: i64,
    ) -> SubgraphResult<handles_query::ResponseData> {
        let variables = handles_query::Variables {
            first,
            cursor_block: cursor_block.to_string(),
        };
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
