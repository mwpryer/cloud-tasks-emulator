use std::sync::Arc;
use std::time::SystemTime;

use aws_lc_rs::digest::{SHA256, digest};
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::rsa::KeySize;
use aws_lc_rs::signature::{KeyPair, RSA_PKCS1_SHA256, RsaKeyPair};
use axum::Json;
use axum::extract::State;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::{Value, json};

/// Signs ID tokens with a key made at startup, which handlers find through discovery
pub(crate) struct Issuer {
    url: String,
    key: RsaKeyPair,
    key_id: String,
}

impl Issuer {
    pub fn new(url: String) -> Self {
        Self {
            url,
            key: RsaKeyPair::generate(KeySize::Rsa2048).expect("RSA key generation succeeds"),
            key_id: format!("{:032x}", rand::random::<u128>()),
        }
    }

    pub fn id_token(&self, service_account_email: &str, audience: &str, now: SystemTime) -> String {
        let issued = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("times are after the epoch")
            .as_secs();
        // Stands in for the service account's numeric unique ID, which only IAM knows
        let hash = digest(&SHA256, service_account_email.as_bytes());
        let unique_id = format!(
            "1{:020}",
            u64::from_be_bytes(hash.as_ref()[..8].try_into().expect("hash has 8 bytes"))
        );
        let header = json!({ "alg": "RS256", "kid": self.key_id, "typ": "JWT" });
        let claims = json!({
            "aud": audience,
            "azp": unique_id,
            "email": service_account_email,
            "email_verified": true,
            "exp": issued + 3600,
            "iat": issued,
            "iss": self.url,
            "sub": unique_id,
        });
        let signed = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(header.to_string()),
            URL_SAFE_NO_PAD.encode(claims.to_string())
        );
        let mut signature = vec![0; self.key.public_modulus_len()];
        self.key
            .sign(
                &RSA_PKCS1_SHA256,
                &SystemRandom::new(),
                signed.as_bytes(),
                &mut signature,
            )
            .expect("signature fits the modulus");
        format!("{signed}.{}", URL_SAFE_NO_PAD.encode(signature))
    }
}

pub(crate) async fn discovery(State(issuer): State<Arc<Issuer>>) -> Json<Value> {
    Json(json!({
        "issuer": issuer.url,
        "jwks_uri": format!("{}/jwks", issuer.url),
        "response_types_supported": ["id_token"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
    }))
}

pub(crate) async fn jwks(State(issuer): State<Arc<Issuer>>) -> Json<Value> {
    let key = issuer.key.public_key();
    Json(json!({
        "keys": [{
            "kty": "RSA",
            "alg": "RS256",
            "use": "sig",
            "kid": issuer.key_id,
            "n": URL_SAFE_NO_PAD.encode(key.modulus().big_endian_without_leading_zero()),
            "e": URL_SAFE_NO_PAD.encode(key.exponent().big_endian_without_leading_zero()),
        }],
    }))
}
