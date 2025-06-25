use serde::Deserialize;
use std::{env, fs};
use std::collections::HashMap;
use std::error::Error;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions};
use yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::prelude::SubscribeRequest;

const PROGRAM_ID_1: &str = "";
const PROGRAM_ID_2: &str = "";

#[derive(Debug, Deserialize)]
struct Config {
    configs: Vec<Auth>,
}

#[derive(Debug, Deserialize, Clone)]
struct Auth {
    end_point: String,
    token: String,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let config_path = args.get(1).expect("Usage: program <config_path>");

    let config_str = fs::read_to_string(config_path).expect("Failed to read config file");
    let config: Config = serde_json::from_str(&config_str).expect("Invalid JSON config format");

    let (sender, mut receiver) = mpsc::channel::<u64>(100);

    for conn in config.configs.clone() {
        let tx = sender.clone();
        tokio::spawn(async move {
            if let Err(e) = worker(conn, tx).await {
                eprintln!("Worker error: {:?}", e);
            }
        });
    }

    drop(sender);

    while let Some(slot) = receiver.recv().await {
        println!("Transactions: {}", slot);
    }
}

async fn worker(conn: Auth, tx: mpsc::Sender<u64>) -> Result<(), Box<dyn Error>> {
    let mut client = GeyserGrpcClient::build_from_shared(conn.end_point.to_string())?
        .x_token(Some(conn.token.to_string()))?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;
    
    let mut transactions: HashMap<String, SubscribeRequestFilterTransactions> = HashMap::new();

    transactions.insert(
        "subscribe_program_1".to_string(),
        SubscribeRequestFilterTransactions {
            vote: None,
            failed: None,
            signature: None,
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![PROGRAM_ID_1.to_string()],
        },
    );

    transactions.insert(
        "subscribe_program_2".to_string(),
        SubscribeRequestFilterTransactions {
            vote: None,
            failed: None,
            signature: None,
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![PROGRAM_ID_2.to_string()],
        },
    );
   
    let subscribe_request = SubscribeRequest {
        transactions,
        ..Default::default()
    };

    let (_sink, mut stream) = client.subscribe_with_request(Some(subscribe_request)).await?;

    while let Some(update) = stream.next().await {
        match update {
            Ok(update) => {
                if let Some(oneof) = update.update_oneof {
                    match oneof {
                        UpdateOneof::Transaction(trans_update) => {
                            let transation = trans_update.transaction;
                            let _ = tx.send(transation.unwrap().index).await;
                        }
                        other => {
                            println!("Other: {:?}", other);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("E: {e}");
                break;
            }
        }
    }
    Ok(())
}
