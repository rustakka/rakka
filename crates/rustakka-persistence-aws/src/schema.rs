//! Idempotent single-table bootstrap.

use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
};
use aws_sdk_dynamodb::Client;
use rustakka_persistence::JournalError;

use crate::config::DynamoConfig;

pub async fn ensure_table(client: &Client, cfg: &DynamoConfig) -> Result<(), JournalError> {
    if !cfg.auto_create_table {
        return Ok(());
    }
    let existing = client
        .list_tables()
        .send()
        .await
        .map_err(|e| JournalError::backend(format!("{e:?}")))?;
    if existing.table_names().iter().any(|n| n == &cfg.table_name) {
        return Ok(());
    }

    let pk = AttributeDefinition::builder()
        .attribute_name("pid")
        .attribute_type(ScalarAttributeType::S)
        .build()
        .map_err(|e| JournalError::backend(format!("{e:?}")))?;
    let sk = AttributeDefinition::builder()
        .attribute_name("sk")
        .attribute_type(ScalarAttributeType::S)
        .build()
        .map_err(|e| JournalError::backend(format!("{e:?}")))?;
    let key_pk = KeySchemaElement::builder()
        .attribute_name("pid")
        .key_type(KeyType::Hash)
        .build()
        .map_err(|e| JournalError::backend(format!("{e:?}")))?;
    let key_sk = KeySchemaElement::builder()
        .attribute_name("sk")
        .key_type(KeyType::Range)
        .build()
        .map_err(|e| JournalError::backend(format!("{e:?}")))?;

    client
        .create_table()
        .table_name(&cfg.table_name)
        .attribute_definitions(pk)
        .attribute_definitions(sk)
        .key_schema(key_pk)
        .key_schema(key_sk)
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .map_err(|e| JournalError::backend(format!("{e:?}")))?;
    Ok(())
}
