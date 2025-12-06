use crate::error::Result;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use url::Url;

pub struct TraceReader {
    ctx: SessionContext,
    bucket: String,
}

impl TraceReader {
    pub async fn new(endpoint: &str, bucket: &str, access_key: &str, secret_key: &str, region: &str) -> Result<Self> {
        let ctx = SessionContext::new();

        let s3 = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_region(region)
            .with_allow_http(true) // Assuming local rustfs might be http
            .build()?;

        let path = format!("s3://{}/traces/", bucket);
        let url = Url::parse(&path).map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        ctx.runtime_env().register_object_store(&url, Arc::new(s3));

        // Register the table
        let table_path = ListingTableUrl::parse(path)?;
        let config = ListingTableConfig::new(table_path).infer_schema(&ctx.state()).await?;

        let table = ListingTable::try_new(config)?;

        ctx.register_table("traces", Arc::new(table))?;

        Ok(Self {
            ctx,
            bucket: bucket.to_string(),
        })
    }

    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(sql).await?;
        let batches = df.collect().await?;
        Ok(batches)
    }
}
