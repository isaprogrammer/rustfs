use crate::error::Result;
use crate::schema::TraceRecord;
use bytes::Bytes;
use chrono::{Datelike, Timelike, Utc};
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use rustfs_ecstore::store_api::{ObjectIO, ObjectOptions, PutObjReader};
use rustfs_rio::{HashReader, WarpReader};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::{error, info};

pub struct TraceWriter {
    tx: mpsc::Sender<TraceRecord>,
    // 用于等待后台 worker 退出；Mutex 是为了能在 &self 上 take()
    done_rx: Mutex<Option<oneshot::Receiver<()>>>,
}

impl TraceWriter {
    pub fn new(bucket: String, max_buffer_size: usize, flush_interval: Duration, store: Arc<dyn ObjectIO>) -> Self {
        let (tx, rx) = mpsc::channel::<TraceRecord>(max_buffer_size * 2);
        let (done_tx, done_rx) = oneshot::channel::<()>();

        // spawn worker，worker 退出前发送 done_tx
        tokio::spawn(async move {
            let mut worker = TraceWriterWorker::new(bucket, max_buffer_size, flush_interval, store);
            worker.run(rx).await;
            // 忽略 send 错误（接收端可能已被 drop）
            let _ = done_tx.send(());
        });

        Self {
            tx,
            done_rx: Mutex::new(Some(done_rx)),
        }
    }

    #[inline]
    pub async fn write(&self, record: TraceRecord) -> Result<()> {
        // 写路径无锁，最高性能
        if let Err(e) = self.tx.send(record).await {
            error!("Failed to flush trace buffer: {:?}", e);
        }
        Ok(())
    }

    /// 等待后台 worker 安全退出并保证所有数据已 flush
    pub async fn shutdown(&self) {
        // drop sender => worker 的 rx.recv() 会在耗尽后返回 None，从而退出 loop
        // 通过克隆后 drop 来释放本 sender 的所有权
        // 当所有 Sender 被 drop 时，Receiver 接收 None 并最终退出 worker
        // 注意：如果你持有其它 Sender clones，也必须 drop 它们
        let _ = std::mem::drop(self.tx.clone());

        // 等待 worker 发回完成信号（如果还能接收的话）
        if let Some(done_rx) = self.done_rx.lock().await.take() {
            let _ = done_rx.await;
        }
    }
}

struct TraceWriterWorker {
    bucket: String,
    store: Arc<dyn ObjectIO>,
    batch_size: usize,
    flush_interval: Duration,

    // 高性能 builder
    trace_id: StringBuilder,
    span_id: StringBuilder,
    parent_span_id: StringBuilder,
    service_name: StringBuilder,
    operation_name: StringBuilder,
    start_time: TimestampMicrosecondBuilder,
    duration_ns: UInt64Builder,
    status_code: Int32Builder,
    status_message: StringBuilder,
    tags_builder: MapBuilder<StringBuilder, StringBuilder>,

    row_count: usize,
}

impl TraceWriterWorker {
    fn new(bucket: String, batch_size: usize, flush_interval: Duration, store: Arc<dyn ObjectIO>) -> Self {
        // 所有 builder 都提前预分配
        Self {
            bucket,
            store,
            batch_size,
            flush_interval,

            trace_id: StringBuilder::with_capacity(batch_size, batch_size * 16),
            span_id: StringBuilder::with_capacity(batch_size, batch_size * 16),
            parent_span_id: StringBuilder::with_capacity(batch_size, batch_size * 16),
            service_name: StringBuilder::with_capacity(batch_size, batch_size * 16),
            operation_name: StringBuilder::with_capacity(batch_size, batch_size * 16),

            start_time: TimestampMicrosecondBuilder::with_capacity(batch_size),
            duration_ns: UInt64Builder::with_capacity(batch_size),
            status_code: Int32Builder::with_capacity(batch_size),
            status_message: StringBuilder::with_capacity(batch_size, batch_size * 16),

            tags_builder: MapBuilder::new(None, StringBuilder::new(), StringBuilder::new()),

            row_count: 0,
        }
    }

    async fn run(&mut self, mut rx: mpsc::Receiver<TraceRecord>) {
        let mut ticker = tokio::time::interval(self.flush_interval);

        loop {
            tokio::select! {
                Some(rec) = rx.recv() => {
                    self.append(rec);
                    if self.row_count >= self.batch_size {
                        if let Err(e) = self.flush().await {
                            error!("flush failed: {:?}", e);
                        }
                    }
                }
                _ = ticker.tick() => {
                    if self.row_count > 0 {
                        if let Err(e) = self.flush().await {
                            error!("periodic flush failed: {:?}", e);
                        }
                    }
                }
            }
        }
    }

    #[inline]
    fn append(&mut self, r: TraceRecord) {
        self.trace_id.append_value(&r.trace_id);
        self.span_id.append_value(&r.span_id);

        match r.parent_span_id {
            Some(p) => self.parent_span_id.append_value(p),
            None => self.parent_span_id.append_null(),
        }

        self.service_name.append_value(&r.service_name);
        self.operation_name.append_value(&r.operation_name);
        self.start_time.append_value(r.start_time.timestamp_micros());
        self.duration_ns.append_value(r.duration_ns);
        self.status_code.append_value(r.status_code);

        match r.status_message {
            Some(msg) => self.status_message.append_value(msg),
            None => self.status_message.append_null(),
        }

        // 更高效的 tag 写法
        for (k, v) in r.tags {
            self.tags_builder.keys().append_value(k);
            self.tags_builder.values().append_value(v);
        }
        self.tags_builder.append(true).unwrap();

        self.row_count += 1;
    }

    async fn flush(&mut self) -> Result<()> {
        if self.row_count == 0 {
            return Ok(());
        }

        let schema = TraceRecord::get_arrow_schema();
        let batch = self.finish_batch(schema.clone())?;

        // Parquet 写入逻辑不改动
        let mut buf = Vec::new();
        let props = WriterProperties::builder().build();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))?;
            writer.write(&batch)?;
            writer.close()?;
        }

        self.upload(buf).await?;

        self.reset_builders();
        Ok(())
    }

    fn finish_batch(&mut self, schema: SchemaRef) -> Result<RecordBatch> {
        let ts = {
            let arr = self.start_time.finish();
            let data = arr.into_data();
            let new_data = data
                .into_builder()
                .data_type(DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())))
                .build()?;
            Arc::new(TimestampMicrosecondArray::from(new_data)) as ArrayRef
        };

        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.trace_id.finish()),
            Arc::new(self.span_id.finish()),
            Arc::new(self.parent_span_id.finish()),
            Arc::new(self.service_name.finish()),
            Arc::new(self.operation_name.finish()),
            ts,
            Arc::new(self.duration_ns.finish()),
            Arc::new(self.status_code.finish()),
            Arc::new(self.status_message.finish()),
            Arc::new(self.tags_builder.finish()),
        ];

        Ok(RecordBatch::try_new(schema, columns)?)
    }

    fn reset_builders(&mut self) {
        self.trace_id = StringBuilder::new();
        self.span_id = StringBuilder::new();
        self.parent_span_id = StringBuilder::new();
        self.service_name = StringBuilder::new();
        self.operation_name = StringBuilder::new();
        self.start_time = TimestampMicrosecondBuilder::new();
        self.duration_ns = UInt64Builder::new();
        self.status_code = Int32Builder::new();
        self.status_message = StringBuilder::new();
        self.tags_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        self.row_count = 0;
    }

    async fn upload(&self, buf: Vec<u8>) -> Result<()> {
        let now = Utc::now();
        let path = format!(
            "traces/year={:04}/month={:02}/day={:02}/hour={:02}/{}.parquet",
            now.year(),
            now.month(),
            now.day(),
            now.hour(),
            uuid::Uuid::new_v4()
        );

        let data = Bytes::from(buf);
        let len = data.len() as i64;
        let cursor = std::io::Cursor::new(data);
        let reader = Box::new(WarpReader::new(cursor));
        let hash_reader =
            HashReader::new(reader, len, len, None, None, false).map_err(|e| crate::error::TraceError::Storage(e.to_string()))?;

        let mut reader = PutObjReader::new(hash_reader);
        let opts = ObjectOptions::default();

        self.store
            .put_object(&self.bucket, &path, &mut reader, &opts)
            .await
            .map_err(|e| crate::error::TraceError::Storage(e.to_string()))?;

        info!("Flushed {} rows to {}/{}", self.row_count, self.bucket, path);

        Ok(())
    }
}
