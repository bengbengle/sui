// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use clap::Parser;
use tracing::{error, info};

use sui_indexer::errors::IndexerError;
use sui_indexer::store::PgIndexerStore;
use sui_indexer::start_prometheus_server;
use sui_indexer::{
    // get_pg_pool_connection, 
    new_pg_connection_pool, 
    Indexer, 
    IndexerConfig
};

#[tokio::main]
async fn main() -> Result<(), IndexerError> {

    let _guard = telemetry_subscribers::TelemetryConfig::new()
        .with_env()
        .init();

    info!("Parsed indexer config: ....");

    let _config = IndexerConfig::parse();
    
    let (blocking_cp, async_cp) = new_pg_connection_pool(&_config.db_url)
        .await
        .map_err(|e| {
            error!(
                "Failed creating Postgres connection pool with error {:?}",
                e
            );
            e
        })?;

    let (_, registry) = start_prometheus_server(
        format!("{}:{}", _config.client_metric_host, _config.client_metric_port).parse().unwrap(), _config.rpc_client_url.as_str()
    )?;

    let _store = PgIndexerStore::new(async_cp, blocking_cp).await;

    Indexer::start(&_config, &registry, _store).await

}
