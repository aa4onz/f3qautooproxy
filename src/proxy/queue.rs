use crate::models::{QueuedItem, ProxyResponse, ReactionDelayMode};
use crate::proxy::state::ProxyState;
use crate::proxy::utils::{generate_snowflake_nonce, parse_leading_number};
use rand::Rng;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::Duration;

pub async fn execute_queued_reaction(
    item: QueuedItem,
    channel_id: String,
    fallback_discord_token: String,
    http_client: Arc<reqwest::Client>,
    gw_broadcast_tx: broadcast::Sender<ProxyResponse>,
    remaining_queue: Vec<QueuedItem>,
    state: ProxyState,
) {
    // Notify connected client of the updated queue state immediately
    let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync {
        queue: remaining_queue,
    });

    let delay_mode = state.get_reaction_delay_mode().await;
    let sending_token = state.get_current_token(&fallback_discord_token).await;

    // Parse number to evaluate swap after sending
    let sent_num = if item.number != 0 {
        Some(item.number)
    } else {
        parse_leading_number(&item.content)
    };

    tokio::spawn(async move {
        // Fire-and-forget typing indicator concurrently in the background so it never slows down response dispatch
        let typing_url = format!("https://discord.com/api/v10/channels/{}/typing", channel_id);
        let client_typing = Arc::clone(&http_client);
        let token_typing = sending_token.clone();
        tokio::spawn(async move {
            let _ = client_typing
                .post(&typing_url)
                .header("Authorization", &token_typing)
                .header("Content-Length", "0")
                .send()
                .await;
        });

        let delay_ms = match delay_mode {
            ReactionDelayMode::Normal => rand::thread_rng().gen_range(200..=300),
            ReactionDelayMode::Fast => rand::thread_rng().gen_range(0..=200),
            ReactionDelayMode::Instant => 0,
            ReactionDelayMode::Random => rand::thread_rng().gen_range(0..=300),
        };

        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }

        let msg_url = format!("https://discord.com/api/v10/channels/{}/messages", channel_id);
        let nonce = generate_snowflake_nonce();
        let payload = serde_json::json!({
            "content": item.content,
            "nonce": nonce
        });

        let res = http_client
            .post(&msg_url)
            .header("Authorization", &sending_token)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await;

        if let Ok(resp) = res {
            if resp.status().is_success() {
                if let Ok(sent_msg) = resp.json::<serde_json::Value>().await {
                    if let Some(msg_id) = sent_msg["id"].as_str() {
                        state.try_mark_message_processed(&channel_id, msg_id).await;
                    }
                    if let Some(author_id) = sent_msg["author"]["id"].as_str() {
                        let author_uname = sent_msg["author"]["username"].as_str().unwrap_or("");
                        state.register_self_info(author_id, author_uname).await;
                    }
                }
                if let Some(num) = sent_num {
                    state.register_sent_number(&channel_id, num).await;
                    state.check_and_swap_token(num).await;
                }
                return;
            }
        }

        // Clear queue on error
        let cleared = state.clear_queue(&channel_id).await;
        let _ = gw_broadcast_tx.send(ProxyResponse::QueueSync { queue: cleared });
    });
}
