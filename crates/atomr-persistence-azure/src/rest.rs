//! Minimal Azure Table REST client.
//!
//! Uses SharedKeyLite signing (sufficient for both production Azure Tables
//! and the Azurite emulator) and wraps only the handful of operations the
//! provider crates need. Not a general-purpose Azure SDK.

use atomr_persistence::JournalError;
use chrono::Utc;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, Method};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::auth::SharedKeySigner;

pub(crate) struct TableClient {
    http: Client,
    signer: SharedKeySigner,
    endpoint: String,
}

impl TableClient {
    pub fn new(
        endpoint: impl Into<String>,
        account: impl Into<String>,
        key_b64: &str,
    ) -> Result<Self, JournalError> {
        let signer = SharedKeySigner::new(account, key_b64).map_err(JournalError::backend)?;
        Ok(Self {
            http: Client::builder().build().map_err(JournalError::backend)?,
            signer,
            endpoint: endpoint.into().trim_end_matches('/').to_string(),
        })
    }

    fn date_header() -> String {
        Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string()
    }

    fn common_headers(
        &self,
        method: Method,
        canonicalized_resource: &str,
    ) -> Result<HeaderMap, JournalError> {
        let mut headers = HeaderMap::new();
        let date = Self::date_header();
        headers.insert("x-ms-date", HeaderValue::from_str(&date).unwrap());
        headers.insert("x-ms-version", HeaderValue::from_static("2019-02-02"));
        headers.insert("Accept", HeaderValue::from_static("application/json;odata=nometadata"));
        headers.insert("Content-Type", HeaderValue::from_static("application/json"));
        let authorization = self.signer.sign_lite(method.as_str(), &date, canonicalized_resource);
        headers
            .insert("Authorization", HeaderValue::from_str(&authorization).map_err(JournalError::backend)?);
        Ok(headers)
    }

    pub async fn create_table_if_absent(&self, name: &str) -> Result<(), JournalError> {
        let canonical = format!("/{}/Tables", self.signer.account());
        let url = format!("{}/Tables", self.endpoint);
        let body = serde_json::json!({ "TableName": name });
        let resp = self
            .http
            .post(&url)
            .headers(self.common_headers(Method::POST, &canonical)?)
            .json(&body)
            .send()
            .await
            .map_err(JournalError::backend)?;
        // 201 created or 409 TableAlreadyExists are both OK
        let status = resp.status();
        if status.is_success() || status.as_u16() == 409 {
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            Err(JournalError::backend(format!("create table: {status} {body}")))
        }
    }

    pub async fn insert_entity<T: Serialize>(&self, table: &str, entity: &T) -> Result<(), JournalError> {
        let canonical = format!("/{}/{}", self.signer.account(), table);
        let url = format!("{}/{}", self.endpoint, table);
        let resp = self
            .http
            .post(&url)
            .headers(self.common_headers(Method::POST, &canonical)?)
            .json(entity)
            .send()
            .await
            .map_err(JournalError::backend)?;
        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            Err(JournalError::backend(format!("insert: {status} {body}")))
        }
    }

    pub async fn upsert_entity<T: Serialize>(
        &self,
        table: &str,
        partition_key: &str,
        row_key: &str,
        entity: &T,
    ) -> Result<(), JournalError> {
        let resource = format!(
            "{table}(PartitionKey='{pk}',RowKey='{rk}')",
            table = table,
            pk = partition_key,
            rk = row_key,
        );
        let canonical = format!("/{}/{}", self.signer.account(), resource);
        let url = format!("{}/{}", self.endpoint, resource);
        let resp = self
            .http
            .put(&url)
            .headers(self.common_headers(Method::PUT, &canonical)?)
            .header("If-Match", "*")
            .json(entity)
            .send()
            .await
            .map_err(JournalError::backend)?;
        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            Err(JournalError::backend(format!("upsert: {status} {body}")))
        }
    }

    pub async fn delete_entity(
        &self,
        table: &str,
        partition_key: &str,
        row_key: &str,
    ) -> Result<(), JournalError> {
        let resource = format!(
            "{table}(PartitionKey='{pk}',RowKey='{rk}')",
            table = table,
            pk = partition_key,
            rk = row_key,
        );
        let canonical = format!("/{}/{}", self.signer.account(), resource);
        let url = format!("{}/{}", self.endpoint, resource);
        let resp = self
            .http
            .delete(&url)
            .headers(self.common_headers(Method::DELETE, &canonical)?)
            .header("If-Match", "*")
            .send()
            .await
            .map_err(JournalError::backend)?;
        let status = resp.status();
        if status.is_success() || status.as_u16() == 404 {
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            Err(JournalError::backend(format!("delete: {status} {body}")))
        }
    }

    pub async fn query_entities<T: DeserializeOwned>(
        &self,
        table: &str,
        filter: &str,
        top: Option<u32>,
    ) -> Result<Vec<T>, JournalError> {
        let canonical = format!("/{}/{}", self.signer.account(), table);
        let mut url = format!("{}/{}()?$filter={}", self.endpoint, table, urlencoding(filter));
        if let Some(t) = top {
            url.push_str(&format!("&$top={t}"));
        }
        let resp = self
            .http
            .get(&url)
            .headers(self.common_headers(Method::GET, &canonical)?)
            .send()
            .await
            .map_err(JournalError::backend)?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(JournalError::backend(format!("query: {status} {body}")));
        }
        let body: Value = resp.json().await.map_err(JournalError::backend)?;
        let values = body.get("value").cloned().unwrap_or_else(|| Value::Array(Vec::new()));
        serde_json::from_value(values).map_err(JournalError::backend)
    }
}

fn urlencoding(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            b' ' => out.push_str("%20"),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urlencoding_preserves_safe_chars() {
        assert_eq!(urlencoding("abc-_.~"), "abc-_.~");
    }

    #[test]
    fn urlencoding_escapes_quotes_and_spaces() {
        let out = urlencoding("PartitionKey eq 'p'");
        assert!(out.contains("%20"));
        assert!(out.contains("%27"));
    }
}
