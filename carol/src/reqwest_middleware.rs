use async_trait::async_trait;
use http::Extensions;
use reqwest_middleware::reqwest::{Body, Request, Response};
use reqwest_middleware::{Middleware, Next};
use tracing::trace;

use crate::pool::Pool;

pub struct CarolMiddleware {
    client_pool: Pool,
}

#[async_trait]
impl Middleware for CarolMiddleware {
    async fn handle(
        &self,
        req: Request,
        extensions: &mut Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<Response> {
        trace!("processing request: {:?}", req);
        let mut client = self.client_pool.get().await.unwrap();
        let file = client.get(req.url()).await.unwrap();

        let mut builder = http::Response::builder();

        let origin_response = next.run(req, extensions).await?.error_for_status()?;
        trace!("origin response: {:?}", origin_response);

        builder = builder.status(origin_response.status());
        builder = builder.version(origin_response.version());
        for header in origin_response.headers() {
            builder = builder.header(header.0, header.1);
        }

        let _stream = origin_response.bytes_stream();
        // TODO: put this stream into cache returning `file`

        let body = Body::from(serde_json::to_string(&file).unwrap());
        let response = Response::from(builder.body(body).unwrap());

        trace!("final response: {:?}", response);
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::CarolMiddleware;
    use crate::pool::{Pool, PoolManager};
    use crate::File;
    use reqwest::Client;
    use reqwest_middleware::ClientBuilder;

    #[tokio::test]
    async fn test_middleware() {
        const DATABASE_URL: &str = "carol.sqlite";
        const CACHE_DIR: &str = ".test-cache";

        let mgr = PoolManager::new(DATABASE_URL, CACHE_DIR);
        let client_pool = Pool::builder(mgr).build().unwrap();

        let reqwest_client = Client::builder().build().unwrap();
        let client = ClientBuilder::new(reqwest_client)
            .with(CarolMiddleware { client_pool })
            .build();
        let response = client.get("https://example.com").send().await.unwrap();
        let file = response.json::<File>().await.unwrap();
        println!("Got file: {:#?}", file);
    }
}
