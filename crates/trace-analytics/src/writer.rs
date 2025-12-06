use crate::error::Result;
use crate::schema::TraceRecord;
use bytes::Bytes;
use chrono::{Datelike, Timelike, Utc};
use datafusion::arrow::array::{ArrayRef, Int32Builder, MapBuilder, StringBuilder, TimestampMicrosecondBuilder, UInt64Builder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::store_api::{ObjectIO, ObjectOptions, PutObjReader};
use rustfs_rio::{HashReader, WarpReader};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info};

pub struct TraceWriter {
    buffer: Arc<Mutex<Vec<TraceRecord>>>,
    max_buffer_size: usize,
    flush_interval: Duration,
    bucket_name: String,
}

impl TraceWriter {
    pub fn new(bucket_name: String, max_buffer_size: usize, flush_interval: Duration) -> Self {
        let writer = Self {
            buffer: Arc::new(Mutex::new(Vec::with_capacity(max_buffer_size))),
            max_buffer_size,
            flush_interval,
            bucket_name,
        };

        writer.start_flush_task();
        writer
    }

    fn start_flush_task(&self) {
        let buffer = self.buffer.clone();
        let interval_duration = self.flush_interval;
        let bucket = self.bucket_name.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval_duration);
            loop {
                interval.tick().await;
                if let Err(e) = Self::flush_buffer(&buffer, &bucket).await {
                    error!("Failed to flush trace buffer: {:?}", e);
                }
            }
        });
    }

    pub async fn write(&self, record: TraceRecord) -> Result<()> {
        let mut buffer = self.buffer.lock().await;
        buffer.push(record);

        if buffer.len() >= self.max_buffer_size {
            // Clone buffer to flush outside lock
            let buffer_to_flush = buffer.clone();
            buffer.clear();
            drop(buffer);

            let bucket = self.bucket_name.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::write_records(buffer_to_flush, &bucket).await {
                    error!("Failed to flush full trace buffer: {:?}", e);
                }
            });
        }
        Ok(())
    }

    async fn flush_buffer(buffer: &Arc<Mutex<Vec<TraceRecord>>>, bucket: &str) -> Result<()> {
        let mut buffer_guard = buffer.lock().await;
        if buffer_guard.is_empty() {
            return Ok(());
        }
        let records = buffer_guard.clone();
        buffer_guard.clear();
        drop(buffer_guard);

        Self::write_records(records, bucket).await
    }

    async fn write_records(records: Vec<TraceRecord>, bucket: &str) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let schema = TraceRecord::get_arrow_schema();
        let batch = Self::records_to_batch(&records, schema.clone())?;

        // Write to Parquet in memory
        let mut buf = Vec::new();
        let props = WriterProperties::builder().build();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))?;
            writer.write(&batch)?;
            writer.close()?;
        }

        // Generate path: traces/year=2023/month=10/day=27/hour=10/{uuid}.parquet
        let now = Utc::now();
        let path = format!(
            "traces/year={:04}/month={:02}/day={:02}/hour={:02}/{}.parquet",
            now.year(),
            now.month(),
            now.day(),
            now.hour(),
            uuid::Uuid::new_v4()
        );

        // Upload to Storage
        let object_layer =
            new_object_layer_fn().ok_or_else(|| crate::error::TraceError::Storage("Object layer not initialized".to_string()))?;

        let data = Bytes::from(buf);
        let len = data.len() as i64;
        let cursor = std::io::Cursor::new(data);
        let reader = Box::new(WarpReader::new(cursor));
        let hash_reader =
            HashReader::new(reader, len, len, None, None, false).map_err(|e| crate::error::TraceError::Storage(e.to_string()))?;

        let mut reader = PutObjReader::new(hash_reader);

        let opts = ObjectOptions::default();

        let _ = object_layer
            .put_object(bucket, &path, &mut reader, &opts)
            .await
            .map_err(|e| crate::error::TraceError::Storage(e.to_string()))?;

        info!("Flushed {} trace records to {}/{}", records.len(), bucket, path);

        Ok(())
    }

    fn records_to_batch(records: &[TraceRecord], schema: datafusion::arrow::datatypes::SchemaRef) -> Result<RecordBatch> {
        let mut trace_id = StringBuilder::new();
        let mut span_id = StringBuilder::new();
        let mut parent_span_id = StringBuilder::new();
        let mut service_name = StringBuilder::new();
        let mut operation_name = StringBuilder::new();
        let mut start_time = TimestampMicrosecondBuilder::new();
        let mut duration_ns = UInt64Builder::new();
        let mut status_code = Int32Builder::new();
        let mut status_message = StringBuilder::new();

        // Map builder for tags: Map<String, String>
        let key_builder = StringBuilder::new();
        let value_builder = StringBuilder::new();
        let mut tags_builder = MapBuilder::new(None, key_builder, value_builder);

        for record in records {
            trace_id.append_value(&record.trace_id);
            span_id.append_value(&record.span_id);
            if let Some(p) = &record.parent_span_id {
                parent_span_id.append_value(p);
            } else {
                parent_span_id.append_null();
            }
            service_name.append_value(&record.service_name);
            operation_name.append_value(&record.operation_name);
            start_time.append_value(record.start_time.timestamp_micros());
            duration_ns.append_value(record.duration_ns);
            status_code.append_value(record.status_code);
            if let Some(m) = &record.status_message {
                status_message.append_value(m);
            } else {
                status_message.append_null();
            }

            // Tags
            for (k, v) in &record.tags {
                tags_builder.keys().append_value(k);
                tags_builder.values().append_value(v);
            }
            tags_builder.append(true)?;
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(trace_id.finish()),
            Arc::new(span_id.finish()),
            Arc::new(parent_span_id.finish()),
            Arc::new(service_name.finish()),
            Arc::new(operation_name.finish()),
            Arc::new(start_time.finish()),
            Arc::new(duration_ns.finish()),
            Arc::new(status_code.finish()),
            Arc::new(status_message.finish()),
            Arc::new(tags_builder.finish()),
        ];

        Ok(RecordBatch::try_new(schema, columns)?)
    }
}
