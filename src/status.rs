use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub const STATUS_SCHEMA: &str = "telora/status";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum StatusType {
    Install,
    Download,
    Unpack,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum StatusState {
    Waiting,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    #[serde(rename = "type")]
    pub ty: StatusType,
    pub key: String,
    pub name: String,
    pub status: StatusState,
    pub tried: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
}

impl Status {
    pub fn to_value(&self) -> serde_json::Result<Value> {
        let mut fields = match serde_json::to_value(self)? {
            Value::Object(fields) => fields,
            _ => unreachable!("Status always serializes as an object"),
        };
        let mut output = Map::new();
        output.insert("schema".into(), Value::String(STATUS_SCHEMA.into()));
        output.append(&mut fields);
        Ok(Value::Object(output))
    }
}

pub fn timestamp() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .expect("RFC 3339 supports every UTC timestamp")
}

pub fn is_status(value: &Value) -> bool {
    value.get("schema").and_then(Value::as_str) == Some(STATUS_SCHEMA)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_status_schema_and_casing_without_an_id() {
        let status = Status {
            ty: StatusType::Download,
            key: "tool-v1".into(),
            name: "Tool archive".into(),
            status: StatusState::Running,
            tried: 1,
            started: Some("2026-08-29T10:20:30Z".into()),
            end: None,
            bytes: Some(1),
            total_bytes: Some(2),
        };
        let value = status.to_value().unwrap();

        assert_eq!(value["schema"], STATUS_SCHEMA);
        assert_eq!(value["type"], "Download");
        assert_eq!(value["status"], "Running");
        assert_eq!(value["totalBytes"], 2);
        assert!(value.get("id").is_none());
        assert!(value.get("end").is_none());
    }
}
