use hmac::{Hmac, Mac};
use sha2::Sha256;
use hex;

pub fn sign_payload(secret: &str, payload: &str) -> Option<String> {
    type HmacSha256 = Hmac<Sha256>;
    if let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) {
        mac.update(payload.as_bytes());
        let result = mac.finalize();
        return Some(hex::encode(result.into_bytes()));
    }
    None
}
