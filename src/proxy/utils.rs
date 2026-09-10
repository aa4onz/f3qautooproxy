use rand::Rng;

/// Extracts a clean Channel ID from either a raw channel ID string or a Discord channel URL.
/// Examples:
/// - "123456789012345678" -> "123456789012345678"
/// - "https://discord.com/channels/111111/222222333333444444" -> "222222333333444444"
/// - "https://canary.discord.com/channels/@me/222222333333444444" -> "222222333333444444"
pub fn extract_channel_id(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.contains('/') {
        if let Some(last_part) = trimmed.split('/').filter(|s| !s.is_empty()).last() {
            let clean_part: String = last_part.chars().filter(|c| c.is_ascii_digit()).collect();
            if !clean_part.is_empty() {
                return clean_part;
            }
        }
    }
    trimmed.chars().filter(|c| c.is_ascii_digit()).collect()
}

/// Generates Discord Snowflake ID formatted string for nonces
pub fn generate_snowflake_nonce() -> String {
    let discord_epoch: u64 = 1420070400000;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let timestamp_part = now_ms.saturating_sub(discord_epoch);
    let worker_id: u64 = 1;
    let process_id: u64 = 1;
    let increment: u64 = rand::thread_rng().gen_range(0..=4095);

    let snowflake = (timestamp_part << 22) | (worker_id << 17) | (process_id << 12) | increment;
    snowflake.to_string()
}

/// Extracts a leading integer from a text string if present
/// Examples:
/// - "1243 nice great" -> 1243
/// - "123a" -> 123
/// - "123withanything" -> 123
pub fn parse_leading_number(text: &str) -> Option<i64> {
    let trimmed = text.trim();
    let mut num_chars = String::new();

    for c in trimmed.chars() {
        if c.is_ascii_digit() {
            num_chars.push(c);
        } else if num_chars.is_empty() && (c == ' ' || c == '_') {
            continue;
        } else {
            break;
        }
    }

    num_chars.parse::<i64>().ok()
}

/// Generates next message text based on incoming content (e.g. "1243 nice" -> "1244", or "1243 gg" -> "1244 ggs!")
pub fn generate_next_count_response(content: &str) -> Option<String> {
    let current_num = parse_leading_number(content)?;
    let next_num = current_num + 1;

    if content.to_lowercase().contains("gg") {
        Some(format!("{} ggs!", next_num))
    } else {
        Some(next_num.to_string())
    }
}
