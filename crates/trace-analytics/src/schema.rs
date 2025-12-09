use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceRecord {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub service_name: String,
    pub operation_name: String,
    pub start_time: DateTime<Utc>,
    pub duration_ns: u64,
    pub status_code: i32,
    pub status_message: Option<String>,
    pub tags: HashMap<String, String>,
}

impl TraceRecord {
    pub fn get_arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("operation_name", DataType::Utf8, false),
            // Timestamp in microseconds
            Field::new("start_time", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("duration_ns", DataType::UInt64, false),
            Field::new("status_code", DataType::Int32, false),
            Field::new("status_message", DataType::Utf8, true),
            // Tags as Map<String, String>
            Field::new(
                "tags",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("keys", DataType::Utf8, false),
                                Field::new("values", DataType::Utf8, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false, // keys sorted
                ),
                true,
            ),
        ]))
    }
}
