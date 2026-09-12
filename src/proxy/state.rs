use crate::models::{QueuedItem, ReactionDelayMode};
use crate::proxy::utils::{format_count_response, parse_leading_number};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Default)]
pub struct ProxyState {
    pub active_queue: Arc<RwLock<HashMap<String, Vec<QueuedItem>>>>,
    pub queue_mode_enabled: Arc<RwLock<bool>>,
    pub hardware_delay_ms: Arc<RwLock<u64>>,
    pub reaction_delay_mode: Arc<RwLock<ReactionDelayMode>>,
    pub self_user_ids: Arc<RwLock<HashSet<String>>>,
    pub self_usernames: Arc<RwLock<HashSet<String>>>,
    pub last_processed_message_id: Arc<RwLock<HashMap<String, String>>>,
    pub available_tokens: Arc<RwLock<Vec<String>>>,
    pub token_swap_count: Arc<RwLock<usize>>,
    pub current_token_index: Arc<RwLock<usize>>,
    pub target_channel_id: Arc<RwLock<String>>,

    // Sequential counting tracking per channel
    pub channel_seq_count: Arc<RwLock<HashMap<String, usize>>>,
    pub channel_flag_seq: Arc<RwLock<HashMap<String, bool>>>,
    pub channel_last_self_num: Arc<RwLock<HashMap<String, i64>>>,
    pub channel_last_num: Arc<RwLock<HashMap<String, i64>>>,
}

impl ProxyState {
    pub fn new() -> Self {
        Self {
            active_queue: Arc::new(RwLock::new(HashMap::new())),
            queue_mode_enabled: Arc::new(RwLock::new(true)),
            hardware_delay_ms: Arc::new(RwLock::new(45u64)),
            reaction_delay_mode: Arc::new(RwLock::new(ReactionDelayMode::Normal)),
            self_user_ids: Arc::new(RwLock::new(HashSet::new())),
            self_usernames: Arc::new(RwLock::new(HashSet::new())),
            last_processed_message_id: Arc::new(RwLock::new(HashMap::new())),
            available_tokens: Arc::new(RwLock::new(Vec::new())),
            token_swap_count: Arc::new(RwLock::new(1)),
            current_token_index: Arc::new(RwLock::new(0)),
            target_channel_id: Arc::new(RwLock::new(String::new())),

            channel_seq_count: Arc::new(RwLock::new(HashMap::new())),
            channel_flag_seq: Arc::new(RwLock::new(HashMap::new())),
            channel_last_self_num: Arc::new(RwLock::new(HashMap::new())),
            channel_last_num: Arc::new(RwLock::new(HashMap::new())),
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

    pub async fn register_self_info(&self, user_id: &str, username: &str) {
        if !user_id.is_empty() {
            self.self_user_ids.write().await.insert(user_id.to_string());
        }
        if !username.is_empty() {
            self.self_usernames.write().await.insert(username.to_string());
        }
    }

    pub async fn is_self_author(&self, author_id: &str, author_uname: &str) -> bool {
        if !author_id.is_empty() && self.self_user_ids.read().await.contains(author_id) {
            return true;
        }
        if !author_uname.is_empty() && self.self_usernames.read().await.contains(author_uname) {
            return true;
        }
        false
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

    /// Atomically checks if `msg_id` was already processed for `channel_id`.
    /// Returns `true` if this caller was the first to mark it as processed,
    /// or `false` if it was already marked as processed previously.
    pub async fn try_mark_message_processed(&self, channel_id: &str, msg_id: &str) -> bool {
        if msg_id.is_empty() {
            return true;
        }
        let mut map = self.last_processed_message_id.write().await;
        if let Some(last_id) = map.get(channel_id) {
            if last_id == msg_id {
                return false;
            }
        }
        map.insert(channel_id.to_string(), msg_id.to_string());
        true
    }

    /// Evaluates the next count response using 4-sequential rule and fallback to (last_self_num + 2).
    pub async fn evaluate_next_count(&self, channel_id: &str, content: &str) -> Option<String> {
        let incoming_num = parse_leading_number(content)?;

        let mut last_num_map = self.channel_last_num.write().await;
        let mut seq_map = self.channel_seq_count.write().await;
        let mut flag_map = self.channel_flag_seq.write().await;
        let self_map = self.channel_last_self_num.read().await;

        let prev_num = last_num_map.get(channel_id).copied();
        let current_flag = *flag_map.get(channel_id).unwrap_or(&false);
        let current_seq = *seq_map.get(channel_id).unwrap_or(&0);

        let is_sequential = match prev_num {
            Some(prev) => incoming_num == prev + 1,
            None => true,
        };

        let new_seq = if is_sequential {
            current_seq + 1
        } else {
            1
        };

        seq_map.insert(channel_id.to_string(), new_seq);
        if new_seq >= 4 {
            flag_map.insert(channel_id.to_string(), true);
        }

        let next_num = if !is_sequential && current_flag {
            if let Some(&last_self) = self_map.get(channel_id) {
                // Reset flag and seq after applying fallback rule
                flag_map.insert(channel_id.to_string(), false);
                seq_map.insert(channel_id.to_string(), 0);
                last_self + 2
            } else {
                incoming_num + 1
            }
        } else {
            incoming_num + 1
        };

        last_num_map.insert(channel_id.to_string(), incoming_num);
        Some(format_count_response(next_num, content))
    }

    /// Registers a sent count number from our user/bot token to update sequence progress.
    pub async fn register_sent_number(&self, channel_id: &str, sent_num: i64) {
        let mut last_self_map = self.channel_last_self_num.write().await;
        let mut last_num_map = self.channel_last_num.write().await;
        let mut seq_map = self.channel_seq_count.write().await;
        let mut flag_map = self.channel_flag_seq.write().await;

        let prev_num = last_num_map.get(channel_id).copied();
        let current_seq = *seq_map.get(channel_id).unwrap_or(&0);

        let is_sequential = match prev_num {
            Some(prev) => sent_num == prev + 1,
            None => true,
        };

        let new_seq = if is_sequential { current_seq + 1 } else { 1 };
        seq_map.insert(channel_id.to_string(), new_seq);
        if new_seq >= 4 {
            flag_map.insert(channel_id.to_string(), true);
        }

        last_self_map.insert(channel_id.to_string(), sent_num);
        last_num_map.insert(channel_id.to_string(), sent_num);
    }
}
