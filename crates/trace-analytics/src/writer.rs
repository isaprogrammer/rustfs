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
use tracing::{error, info, warn};

/// Worker 控制消息
enum WorkerMsg {
    Record(TraceRecord),
    Shutdown,
}

pub struct TraceWriter {
    tx: Mutex<Option<mpsc::Sender<WorkerMsg>>>,
    done_rx: Mutex<Option<oneshot::Receiver<()>>>,
}

impl TraceWriter {
    pub fn new(
        bucket: String,
        max_buffer_size: usize,
        flush_interval: Duration,
        store: Arc<dyn ObjectIO>,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<WorkerMsg>(max_buffer_size * 2);
        let (done_tx, done_rx) = oneshot::channel::<()>();

        tokio::spawn(async move {
            let mut worker = TraceWriterWorker::new(bucket, max_buffer_size, flush_interval, store);
            worker.run(rx).await;
            let _ = done_tx.send(());
        });

        Self {
            tx: Mutex::new(Some(tx)),
            done_rx: Mutex::new(Some(done_rx)),
        }
    }

    #[inline]
    pub async fn write(&self, record: TraceRecord) -> Result<()> {
        let tx = self.tx.lock().await;
        if let Some(sender) = tx.as_ref() {
            sender
                .send(WorkerMsg::Record(record))
                .await
                .map_err(|e| crate::error::TraceError::Storage(format!("Channel send failed: {:?}", e)))?;
            Ok(())
        } else {
            Err(crate::error::TraceError::Storage(
                "TraceWriter已关闭".to_string(),
            ))
        }
    }

    /// 等待后台 worker 安全退出并保证所有数据已 flush
    pub async fn shutdown(&self) -> Result<()> {
        // 1. 取出并 drop sender，发送关闭信号
        let tx = self.tx.lock().await.take();
        if let Some(sender) = tx {
            // 发送显式关闭消息（可选，也可以直接 drop）
            let _ = sender.send(WorkerMsg::Shutdown).await;
            drop(sender);
        }

        // 2. 等待 worker 完成
        if let Some(done_rx) = self.done_rx.lock().await.take() {
            done_rx
                .await
                .map_err(|e| crate::error::TraceError::Storage(format!("Worker shutdown failed: {:?}", e)))?;
        }

        info!("TraceWriter shutdown完成");
        Ok(())
    }
}

impl Drop for TraceWriter {
    fn drop(&mut self) {
        // 检查是否有未完成的 shutdown
        let tx = self.tx.blocking_lock();
        let done_rx = self.done_rx.blocking_lock();

        if tx.is_some() || done_rx.is_some() {
            warn!(
                "TraceWriter dropped without proper shutdown! 可能有数据未刷盘。\
                 请确保在 drop 前调用 shutdown().await"
            );
        }
    }
}

struct TraceWriterWorker {
    bucket: String,
    store: Arc<dyn ObjectIO>,
    batch_size: usize,
    flush_interval: Duration,

    // 高性能 builder - 保留容量配置
    builder_capacity: usize,
    builder_data_capacity: usize,

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
    fn new(
        bucket: String,
        batch_size: usize,
        flush_interval: Duration,
        store: Arc<dyn ObjectIO>,
    ) -> Self {
        let builder_capacity = batch_size;
        let builder_data_capacity = batch_size * 16;

        Self {
            bucket,
            store,
            batch_size,
            flush_interval,
            builder_capacity,
            builder_data_capacity,

            trace_id: StringBuilder::with_capacity(builder_capacity, builder_data_capacity),
            span_id: StringBuilder::with_capacity(builder_capacity, builder_data_capacity),
            parent_span_id: StringBuilder::with_capacity(builder_capacity, builder_data_capacity),
            service_name: StringBuilder::with_capacity(builder_capacity, builder_data_capacity),
            operation_name: StringBuilder::with_capacity(builder_capacity, builder_data_capacity),

            start_time: TimestampMicrosecondBuilder::with_capacity(builder_capacity)
                .with_timezone("UTC"),
            duration_ns: UInt64Builder::with_capacity(builder_capacity),
            status_code: Int32Builder::with_capacity(builder_capacity),
            status_message: StringBuilder::with_capacity(builder_capacity, builder_data_capacity),

            tags_builder: MapBuilder::new(None, StringBuilder::new(), StringBuilder::new()),

            row_count: 0,
        }
    }

    async fn run(&mut self, mut rx: mpsc::Receiver<WorkerMsg>) {
        let mut ticker = tokio::time::interval(self.flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(WorkerMsg::Record(rec)) => {
                            self.append(rec);
                            if self.row_count >= self.batch_size {
                                if let Err(e) = self.flush().await {
                                    error!("flush failed: {:?}", e);
                                }
                            }
                        }
                        Some(WorkerMsg::Shutdown) | None => {
                            // 收到关闭信号或 channel 关闭
                            info!("TraceWriter worker收到关闭信号，准备最终flush");
                            if self.row_count > 0 {
                                if let Err(e) = self.flush().await {
                                    error!("最终flush失败: {:?}", e);
                                } else {
                                    info!("最终flush成功");
                                }
                            }
                            break;
                        }
                    }
                }
                _ = ticker.tick() => {
                    if self.row_count > 0 {
                        if let Err(e) = self.flush().await {
                            error!("定期flush失败: {:?}", e);
                        }
                    }
                }
            }
        }

        info!("TraceWriter worker退出");
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
        // 直接使用带时区的 TimestampMicrosecondArray，无需额外转换
        let ts = Arc::new(self.start_time.finish()) as ArrayRef;

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
        // 保留容量，避免重新分配
        let cap = self.builder_capacity;
        let data_cap = self.builder_data_capacity;

        self.trace_id = StringBuilder::with_capacity(cap, data_cap);
        self.span_id = StringBuilder::with_capacity(cap, data_cap);
        self.parent_span_id = StringBuilder::with_capacity(cap, data_cap);
        self.service_name = StringBuilder::with_capacity(cap, data_cap);
        self.operation_name = StringBuilder::with_capacity(cap, data_cap);

        self.start_time = TimestampMicrosecondBuilder::with_capacity(cap)
            .with_timezone("UTC");
        self.duration_ns = UInt64Builder::with_capacity(cap);
        self.status_code = Int32Builder::with_capacity(cap);
        self.status_message = StringBuilder::with_capacity(cap, data_cap);

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
        let hash_reader = HashReader::new(reader, len, len, None, None, false)
            .map_err(|e| crate::error::TraceError::Storage(e.to_string()))?;

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