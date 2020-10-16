use opentelemetry::api::{Key, Value};
use opentelemetry::exporter::trace;
use opentelemetry::sdk::trace::evicted_hash_map::EvictedHashMap;
use std::fmt;
use std::sync::Arc;

mod v03;
mod v05;

pub(crate) const DEFAULT_INSTRUMENT_NAME: &str =
    "opentelemetry-instrumentation-rust-contrib-datadog";

// TODO @achang: This is terrible/temporary until opentelemetry-contrib and the next version of
// opentelemetry is actually released.
pub(crate) fn get_span_attribute<'a>(
    attributes: &'a EvictedHashMap,
    key: &'a Key,
) -> Option<&'a Value> {
    attributes
        .iter()
        .find(|attr| attr.0 == key)
        .map(|attr| attr.1)
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Error {
    MessagePackError,
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::MessagePackError => write!(f, "message pack error"),
        }
    }
}

impl From<rmp::encode::ValueWriteError> for Error {
    fn from(_: rmp::encode::ValueWriteError) -> Self {
        Self::MessagePackError
    }
}

/// Version of datadog trace ingestion API
#[derive(Debug, Copy, Clone)]
pub enum ApiVersion {
    /// Version 0.3
    Version03,
    /// Version 0.5 - requires datadog-agent v7.22.0 or above
    Version05,
}

impl ApiVersion {
    pub(crate) fn path(self) -> &'static str {
        match self {
            ApiVersion::Version03 => "/v0.3/traces",
            ApiVersion::Version05 => "/v0.5/traces",
        }
    }

    pub(crate) fn content_type(self) -> &'static str {
        match self {
            ApiVersion::Version03 => "application/msgpack",
            ApiVersion::Version05 => "application/msgpack",
        }
    }

    pub(crate) fn encode(
        self,
        service_name: &str,
        spans: Vec<Arc<trace::SpanData>>,
    ) -> Result<Vec<u8>, Error> {
        match self {
            Self::Version03 => v03::encode(service_name, spans),
            Self::Version05 => v05::encode(service_name, spans),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::api::Key;
    use opentelemetry::{api, sdk};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    fn get_spans() -> Vec<Arc<trace::SpanData>> {
        let parent_span_id = 1;
        let trace_id = 7;
        let span_id = 99;

        let span_context = api::trace::span_context::SpanContext::new(
            api::trace::span_context::TraceId::from_u128(trace_id),
            api::trace::span_context::SpanId::from_u64(span_id),
            0,
            false,
        );

        let start_time = SystemTime::UNIX_EPOCH;
        let end_time = start_time.checked_add(Duration::from_secs(1)).unwrap();

        let capacity = 3;
        let mut attributes = sdk::trace::evicted_hash_map::EvictedHashMap::new(capacity);
        attributes.insert(Key::new("span.type").string("web"));

        let message_events = sdk::trace::evicted_queue::EvictedQueue::new(capacity);
        let links = sdk::trace::evicted_queue::EvictedQueue::new(capacity);

        let span_data = trace::SpanData {
            span_context,
            parent_span_id: api::trace::span_context::SpanId::from_u64(parent_span_id),
            span_kind: api::trace::span::SpanKind::Client,
            name: "resource".to_string(),
            start_time,
            end_time,
            attributes,
            message_events,
            links,
            status_code: api::trace::span::StatusCode::OK,
            status_message: String::new(),
            resource: Arc::new(sdk::Resource::default()),
        };

        vec![Arc::new(span_data)]
    }

    #[test]
    fn test_encode_v03() -> Result<(), Box<dyn std::error::Error>> {
        let spans = get_spans();
        let encoded = base64::encode(ApiVersion::Version03.encode("service_name", spans)?);

        assert_eq!(encoded.as_str(), "kZGLpHR5cGWjd2Vip3NlcnZpY2Wsc2VydmljZV9uYW1lpG5hbWXZMm9wZW50ZWxlbWV0cnktaW5zdHJ1bWVudGF0aW9uLXJ1c3QtY29udHJpYi1kYXRhZG9nqHJlc291cmNlqHJlc291cmNlqHRyYWNlX2lkzwAAAAAAAAAHp3NwYW5faWTPAAAAAAAAAGOpcGFyZW50X2lkzwAAAAAAAAABpXN0YXJ00wAAAAAAAAAAqGR1cmF0aW9u0wAAAAA7msoApWVycm9y0gAAAACkbWV0YYGpc3Bhbi50eXBlo3dlYg==");

        Ok(())
    }

    #[test]
    fn test_encode_v05() -> Result<(), Box<dyn std::error::Error>> {
        let spans = get_spans();
        let encoded = base64::encode(ApiVersion::Version05.encode("service_name", spans)?);

        assert_eq!(encoded.as_str(), "kpWsc2VydmljZV9uYW1lo3dlYtkyb3BlbnRlbGVtZXRyeS1pbnN0cnVtZW50YXRpb24tcnVzdC1jb250cmliLWRhdGFkb2eocmVzb3VyY2Wpc3Bhbi50eXBlkZGczgAAAADOAAAAAs4AAAADzwAAAAAAAAAHzwAAAAAAAABjzwAAAAAAAAAB0wAAAAAAAAAA0wAAAAA7msoA0gAAAACBzgAAAATOAAAAAYDOAAAAAQ==");

        Ok(())
    }
}
