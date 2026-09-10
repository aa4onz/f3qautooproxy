use crate::models::{QueuedItem, ReactionDelayMode};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Default)]
pub struct ProxyState {
    pub active_queue: Arc<RwLock<HashMap<String, Vec<QueuedItem>>>>,
    pub queue_mode_enabled: Arc<RwLock<bool>>,
    pub hardware_delay_ms: Arc<RwLock<u64>>,
    pub reaction_delay_mode: Arc<RwLock<ReactionDelayMode>>,
    pub self_user_id: Arc<RwLock<String>>,
    pub self_username: Arc<RwLock<String>>,
    pub last_processed_message_id: Arc<RwLock<HashMap<String, String>>>,
    pub available_tokens: Arc<RwLock<Vec<String>>>,
    pub token_swap_count: Arc<RwLock<usize>>,
    pub current_token_index: Arc<RwLock<usize>>,
    pub target_channel_id: Arc<RwLock<String>>,
}

impl ProxyState {
    pub fn new() -> Self {
        Self {
            active_queue: Arc::new(RwLock::new(HashMap::new())),
            queue_mode_enabled: Arc::new(RwLock::new(true)),
            hardware_delay_ms: Arc::new(RwLock::new(45u64)),
            reaction_delay_mode: Arc::new(RwLock::new(ReactionDelayMode::Normal)),
            self_user_id: Arc::new(RwLock::new(String::new())),
            self_username: Arc::new(RwLock::new(String::new())),
            last_processed_message_id: Arc::new(RwLock::new(HashMap::new())),
            available_tokens: Arc::new(RwLock::new(Vec::new())),
            token_swap_count: Arc::new(RwLock::new(1)),
            current_token_index: Arc::new(RwLock::new(0)),
            target_channel_id: Arc::new(RwLock::new(String::new())),
        }
    }

    pub async fn set_target_channel_id(&self, channel_id: &str) {
        *self.target_channel_id.write().await = channel_id.trim().to_string();
    }

    pub async fn get_target_channel_id(&self) -> String {
        self.target_channel_id.read().await.clone()
    }

    pub async fn set_token_rotation_config(&self, tokens: Vec<String>, swap_count: usize) {
        *self.available_tokens.write().await = tokens;
        *self.token_swap_count.write().await = swap_count;
        *self.current_token_index.write().await = 0;
    }

    pub async fn get_current_token(&self, fallback: &str) -> String {
        let tokens = self.available_tokens.read().await;
        if tokens.is_empty() {
            return fallback.to_string();
        }
        let idx = *self.current_token_index.read().await;
        tokens.get(idx).cloned().unwrap_or_else(|| fallback.to_string())
    }

    pub async fn check_and_swap_token(&self, number: i64) {
        let swap_count = *self.token_swap_count.read().await;
        let tokens = self.available_tokens.read().await;
        let total_tokens = tokens.len();

        if swap_count <= 1 || total_tokens <= 1 {
            return;
        }

        let num_abs = number.abs();
        let active_swap_count = swap_count.min(total_tokens);

        let should_swap = match active_swap_count {
            2 => num_abs % 100 == 0,
            3 => num_abs % 50 == 0,
            _ => false,
        };

        if should_swap {
            let mut idx_lock = self.current_token_index.write().await;
            let next_idx = (*idx_lock + 1) % active_swap_count;
            *idx_lock = next_idx;
            println!("[TOKEN SWAP] Triggered at number {}. Switched to token #{}.", number, next_idx + 1);
        }
    }

    pub async fn is_queue_mode_enabled(&self) -> bool {
        *self.queue_mode_enabled.read().await
    }

    pub async fn set_queue_mode(&self, enabled: bool) {
        *self.queue_mode_enabled.write().await = enabled;
        if !enabled {
            self.clear_all_queues().await;
        }
    }

    pub async fn set_hardware_delay(&self, delay_ms: u64) {
        *self.hardware_delay_ms.write().await = delay_ms;
    }

    pub async fn set_reaction_delay_mode(&self, mode: ReactionDelayMode) {
        *self.reaction_delay_mode.write().await = mode;
    }

    pub async fn get_reaction_delay_mode(&self) -> ReactionDelayMode {
        *self.reaction_delay_mode.read().await
    }

    pub async fn clear_all_queues(&self) {
        let mut map = self.active_queue.write().await;
        for q in map.values_mut() {
            q.clear();
        }
    }

    pub async fn clear_queue(&self, channel_id: &str) -> Vec<QueuedItem> {
        let mut map = self.active_queue.write().await;
        if let Some(q) = map.get_mut(channel_id) {
            q.clear();
        }
        Vec::new()
    }

    pub async fn enqueue_item(&self, channel_id: &str, mut item: QueuedItem) -> Vec<QueuedItem> {
        let mut map = self.active_queue.write().await;
        let q = map.entry(channel_id.to_string()).or_default();
        if q.is_empty() {
            item.was_empty = true;
        }
        q.push(item);
        q.clone()
    }

    pub async fn pop_next_item(&self, channel_id: &str) -> Option<(QueuedItem, Vec<QueuedItem>)> {
        let mut map = self.active_queue.write().await;
        if let Some(q) = map.get_mut(channel_id) {
            if !q.is_empty() {
                let item = q.remove(0);
                return Some((item, q.clone()));
            }
        }
        None
    }

    pub async fn set_self_info(&self, user_id: &str, username: &str) {
        if !user_id.is_empty() {
            *self.self_user_id.write().await = user_id.to_string();
        }
        if !username.is_empty() {
            *self.self_username.write().await = username.to_string();
        }
    }

    pub async fn is_self_author(&self, author_id: &str, author_uname: &str) -> bool {
        let my_id = self.self_user_id.read().await;
        let my_uname = self.self_username.read().await;
        (!my_id.is_empty() && author_id == *my_id) || (!my_uname.is_empty() && author_uname == *my_uname)
    }

    pub async fn is_message_already_processed(&self, channel_id: &str, msg_id: &str) -> bool {
        if msg_id.is_empty() {
            return false;
        }
        let map = self.last_processed_message_id.read().await;
        if let Some(last_id) = map.get(channel_id) {
            last_id == msg_id
        } else {
            false
        }
    }

    pub async fn set_last_processed_message_id(&self, channel_id: &str, msg_id: &str) {
        if msg_id.is_empty() {
            return;
        }
        let mut map = self.last_processed_message_id.write().await;
        map.insert(channel_id.to_string(), msg_id.to_string());
    }
}
