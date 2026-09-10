use fast_discord_tui::models::{ProxyResponse, ReactionDelayMode};
use fast_discord_tui::proxy::client_handler::handle_client_connection;
use fast_discord_tui::proxy::discord_gw::{run_discord_gateway, SharedGwWriter};
use fast_discord_tui::proxy::state::ProxyState;
use fast_discord_tui::proxy::trigger_rules::evaluate_and_trigger_queue;
use fast_discord_tui::proxy::utils::extract_channel_id;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, ACCEPT_LANGUAGE, USER_AGENT};
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};
use tokio::time::Duration;

fn save_env_var(file_path: &Path, key: &str, value: &str) {
    let clean_val = value.trim().trim_matches('"').trim_matches('\'');
    let mut lines: Vec<String> = if file_path.exists() {
        fs::read_to_string(file_path)
            .unwrap_or_default()
            .lines()
            .map(|s| s.to_string())
            .collect()
    } else {
        Vec::new()
    };

    let mut found = false;
    let new_line = format!("{}=\"{}\"", key, clean_val);

    for line in lines.iter_mut() {
        let trimmed = line.trim_start();
        if trimmed.starts_with(&format!("{}=", key)) {
            *line = new_line.clone();
            found = true;
            break;
        }
    }

    if !found {
        lines.push(new_line);
    }

    let content = lines.join("\n") + "\n";
    if let Err(e) = fs::write(file_path, content) {
        println!("[!] Failed to save {} to {:?}: {}", key, file_path, e);
    } else {
        println!("[+] Saved {} to {:?}", key, file_path);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let profile_num = if args.len() > 1 {
        let arg = &args[1];
        if arg == "--profile" && args.len() > 2 {
            args[2].clone()
        } else {
            arg.trim_start_matches('-').to_string()
        }
    } else {
        env::var("PROFILE").unwrap_or_else(|_| "1".to_string())
    };

    let profile_env_filename = format!(".env.profile_{}", profile_num);
    let target_env_file = if Path::new(&profile_env_filename).exists() {
        Path::new(&profile_env_filename).to_path_buf()
    } else {
        Path::new(".env").to_path_buf()
    };

    if target_env_file.exists() {
        if let Err(e) = dotenvy::from_path(&target_env_file) {
            println!("[!] Failed to parse {:?}: {}", target_env_file, e);
        } else {
            println!("Loaded configuration from {:?}", target_env_file);
        }
    } else {
        println!("[!] {:?} was not found in working directory ({:?})", target_env_file, env::current_dir().unwrap_or_default());
    }

    let mut token_prompted = false;
    let raw_tokens = match env::var("DISCORD_TOKEN") {
        Ok(val) if !val.trim().is_empty() => val.trim().trim_matches('"').to_string(),
        _ => {
            token_prompted = true;
            println!("\n[!] DISCORD_TOKEN not found in environment or loaded .env file.");
            print!("Please enter your Discord token(s) [comma-separated for multi-token]: ");
            io::stdout().flush()?;
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            let trimmed = input.trim().trim_matches('"').to_string();
            if trimmed.is_empty() {
                panic!("No Discord token provided. Exiting.");
            }
            trimmed
        }
    };

    if token_prompted {
        save_env_var(&target_env_file, "DISCORD_TOKEN", &raw_tokens);
    }

    let tokens: Vec<String> = raw_tokens
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if tokens.is_empty() {
        panic!("No valid tokens found in DISCORD_TOKEN");
    }

    let mut channel_prompted = false;
    let raw_channel_input = match env::var("CHANNEL_ID") {
        Ok(val) if !val.trim().is_empty() => val.trim().trim_matches('"').to_string(),
        _ => {
            channel_prompted = true;
            print!("\nEnter Target Channel ID or Link to auto-count in: ");
            io::stdout().flush()?;
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            input.trim().trim_matches('"').to_string()
        }
    };

    if channel_prompted && !raw_channel_input.is_empty() {
        save_env_var(&target_env_file, "CHANNEL_ID", &raw_channel_input);
    }

    let channel_id = extract_channel_id(&raw_channel_input);

    // Prompt user in terminal for token swap selection
    println!("\n==========================================");
    println!("Available Tokens: {}", tokens.len());
    println!("Target Channel ID: {}", if channel_id.is_empty() { "None (Waiting for socket)" } else { &channel_id });
    println!("Select token swap mode:");
    println!("  1 = No swap (use Token 1 only)");
    println!("  2 = Swap 2 tokens (Swap on numbers ending in 00)");
    println!("  3 = Swap 3 tokens (Swap on numbers ending in 50 or 00)");
    print!("Enter choice (1, 2, or 3) [default: 1]: ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let swap_count: usize = match input.trim().parse::<usize>() {
        Ok(val) if (1..=3).contains(&val) => val,
        _ => 1,
    };

    println!("\nSelect reaction delay mode:");
    println!("  1 = Normal (200ms - 300ms delay)");
    println!("  2 = Fast (0ms - 200ms delay)");
    println!("  3 = Instant (0ms delay)");
    print!("Enter choice (1, 2, or 3) [default: 1]: ");
    io::stdout().flush()?;

    let mut delay_input = String::new();
    io::stdin().read_line(&mut delay_input)?;
    let delay_mode = match delay_input.trim() {
        "2" => ReactionDelayMode::Fast,
        "3" => ReactionDelayMode::Instant,
        _ => ReactionDelayMode::Normal,
    };

    println!("Selected swap mode: {}", swap_count);
    println!("Selected delay mode: {:?}", delay_mode);
    println!("==========================================\n");

    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("0.0.0.0:{}", port);

    let listener = TcpListener::bind(&addr).await?;
    println!(
        "Remote proxy server (Profile {}) listening on ws://{} with {} token(s)",
        profile_num,
        addr,
        tokens.len()
    );

    let (gw_tx, _) = broadcast::channel::<ProxyResponse>(512);

    let proxy_state = ProxyState::new();
    proxy_state.set_token_rotation_config(tokens.clone(), swap_count).await;
    proxy_state.set_target_channel_id(&channel_id).await;
    proxy_state.set_reaction_delay_mode(delay_mode).await;

    // HTTP Client initialization
    let mut default_headers = HeaderMap::new();
    default_headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
    default_headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));
    default_headers.insert(
        USER_AGENT,
        HeaderValue::from_static(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        ),
    );

    let http_client = Arc::new(
        reqwest::Client::builder()
            .tcp_nodelay(true)
            .tcp_keepalive(Duration::from_secs(15))
            .default_headers(default_headers)
            .build()?,
    );

    // Spawn Gateway WebSocket Loop for each configured token
    for token in &tokens {
        let gw_broadcast_tx = gw_tx.clone();
        let token_clone = token.clone();
        let client_ref = Arc::clone(&http_client);
        let gw_writer_arc: SharedGwWriter = Arc::new(Mutex::new(None));
        let state_clone = proxy_state.clone();

        tokio::spawn(async move {
            run_discord_gateway(
                token_clone,
                state_clone,
                gw_broadcast_tx,
                client_ref,
                gw_writer_arc,
            )
            .await;
        });
    }

    let primary_token = tokens[0].clone();

    // Check last channel message once on startup if channel ID provided
    if !channel_id.is_empty() {
        let state_init = proxy_state.clone();
        let client_init = Arc::clone(&http_client);
        let token_init = primary_token.clone();
        let gw_tx_init = gw_tx.clone();
        let cid_init = channel_id.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            evaluate_and_trigger_queue(
                None,
                &cid_init,
                &state_init,
                token_init,
                client_init,
                gw_tx_init,
            )
            .await;
        });
    }

    while let Ok((stream, _)) = listener.accept().await {
        let _ = stream.set_nodelay(true);
        let token = primary_token.clone();
        let client = Arc::clone(&http_client);
        let gw_tx_clone = gw_tx.clone();
        let state_conn = proxy_state.clone();

        tokio::spawn(async move {
            handle_client_connection(stream, token, client, state_conn, gw_tx_clone).await;
        });
    }

    Ok(())
}
