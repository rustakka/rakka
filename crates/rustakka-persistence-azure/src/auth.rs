//! Azure Storage Shared Key signer (lite variant for Table Storage).
//!
//! Implements the subset of the signing rules needed for REST operations
//! the provider invokes (GET / POST / MERGE / DELETE on table resources).

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub struct SharedKeySigner {
    account: String,
    decoded_key: Vec<u8>,
}

impl SharedKeySigner {
    pub fn new(account: impl Into<String>, key_b64: &str) -> Result<Self, String> {
        let decoded = B64.decode(key_b64).map_err(|e| e.to_string())?;
        Ok(Self { account: account.into(), decoded_key: decoded })
    }

    /// Produce the `Authorization` header value for the given request.
    ///
    /// `canonicalized_resource` must start with `/{account}` and include
    /// the resource path (e.g. `/devstoreaccount1/Tables`).
    pub fn sign_lite(
        &self,
        method: &str,
        date_header: &str,
        canonicalized_resource: &str,
    ) -> String {
        let string_to_sign =
            format!("{method}\n\napplication/json\n{date_header}\n{canonicalized_resource}");
        let mut mac = HmacSha256::new_from_slice(&self.decoded_key).expect("hmac key");
        mac.update(string_to_sign.as_bytes());
        let sig = B64.encode(mac.finalize().into_bytes());
        format!("SharedKeyLite {account}:{sig}", account = self.account)
    }

    pub fn account(&self) -> &str {
        &self.account
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signs_produces_stable_output() {
        let key = B64.encode(b"0123456789abcdef0123456789abcdef");
        let signer = SharedKeySigner::new("acct", &key).unwrap();
        let a =
            signer.sign_lite("GET", "Mon, 01 Jan 2024 00:00:00 GMT", "/acct/Tables");
        let b =
            signer.sign_lite("GET", "Mon, 01 Jan 2024 00:00:00 GMT", "/acct/Tables");
        assert_eq!(a, b, "signer should be deterministic");
        assert!(a.starts_with("SharedKeyLite acct:"));
    }

    #[test]
    fn rejects_bad_key() {
        assert!(SharedKeySigner::new("acct", "not-base64!!").is_err());
    }
}
