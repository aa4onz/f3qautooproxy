use crate::models::ProxyResponse;
use crate::proxy::queue::execute_queued_reaction;
use crate::proxy::state::ProxyState;
use crate::proxy::utils::generate_next_count_response;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Rules governing stealth counter and queue reactions:
/// 1. Triggers on incoming channel message from other users (non-bot, non-self).
/// 2. If incoming text contains a leading number (e.g. "1243 nice great"), calculates next number (1244).
/// 3. If text contains "gg", formats response as "1244 ggs!", otherwise "1244".
/// 4. Falls back to queue if manually queued items exist.
pub async fn evaluate_and_trigger_queue(
    message_data: Option<&serde_json::Value>,
    channel_id: &str,
    state: &ProxyState,
    discord_token: String,
    http_client: Arc<reqwest::Client>,
    gw_broadcast_tx: broadcast::Sender<ProxyResponse>,
) {
    if channel_id.is_empty() {
        return;
    }

    // Fetch last message from Discord API if no direct gateway event was passed
    let fetched_last_msg;
    let data = match message_data {
        Some(d) => d,
        None => {
            let url = format!(
                "https://discord.com/api/v10/channels/{}/messages?limit=1",
                channel_id
            );
            let res = http_client
                .get(&url)
                .header("Authorization", &discord_token)
                .send()
                .await;

            if let Ok(resp) = res {
                if let Ok(arr) = resp.json::<serde_json::Value>().await {
                    if let Some(first_msg) = arr.get(0) {
                        fetched_last_msg = first_msg.clone();
                        &fetched_last_msg
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            } else {
                return;
            }
        }
    };

    let msg_id = data["id"].as_str().unwrap_or("");
    if !msg_id.is_empty() && state.is_message_already_processed(channel_id, msg_id).await {
        return;
    }

    let author_id = data["author"]["id"].as_str().unwrap_or("");
    let author_uname = data["author"]["username"].as_str().unwrap_or("");
    let is_bot = data["author"]["bot"].as_bool().unwrap_or(false);
    let content = data["content"].as_str().unwrap_or("");

    // Do not trigger on own message or bot messages
    if state.is_self_author(author_id, author_uname).await || is_bot {
        return;
    }

    // Mark message ID as processed
    if !msg_id.is_empty() {
        state.set_last_processed_message_id(channel_id, msg_id).await;
    }

    // Auto-count check: if incoming message has a number ("1243 nice" -> "1244" or "1243 gg" -> "1244 ggs!")
    if let Some(response_text) = generate_next_count_response(content) {
        let auto_item = crate::models::QueuedItem {
            content: response_text,
            number: 0,
            was_empty: false,
        };

        execute_queued_reaction(
            auto_item,
            channel_id.to_string(),
            discord_token,
            http_client,
            gw_broadcast_tx,
            Vec::new(),
            state.clone(),
        )
        .await;
        return;
    }

    // Fallback trigger if queue items exist in state
    if let Some((item, remaining_q)) = state.pop_next_item(channel_id).await {
        execute_queued_reaction(
            item,
            channel_id.to_string(),
            discord_token,
            http_client,
            gw_broadcast_tx,
            remaining_q,
            state.clone(),
        )
        .await;
    }
}
