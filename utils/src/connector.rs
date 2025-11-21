use crate::config::RedisConfig;
use sea_orm::{Database, DatabaseConnection};

pub fn create_redis_pool(redis_config: &RedisConfig) -> Option<deadpool_redis::Pool> {
    let cfg = deadpool_redis::Config { connection: Some(deadpool_redis::ConnectionInfo {
        addr: deadpool_redis::ConnectionAddr::Tcp(
            redis_config.redis_host.clone(),
            redis_config.redis_port,
        ),
        redis: deadpool_redis::RedisConnectionInfo {
            db: redis_config.redis_db as i64,
            username: redis_config.redis_username.clone(),
            password: redis_config.redis_password.clone(),
            protocol: deadpool_redis::ProtocolVersion::RESP3,
        },
    }), ..Default::default() };
    cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1)).ok()
}
pub async fn postgres_connection(
    config: &crate::config::PostgresConfig,
) -> Option<DatabaseConnection> {
    let pg_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        config.database_user,
        config.database_password,
        config.database_host,
        config.database_port,
        config.database_name
    );

    let mut db_options = sea_orm::ConnectOptions::new(pg_url);
    db_options
        .set_schema_search_path(&config.database_schema)
        .sqlx_logging(true)
        // Show SQL statements and bound parameters. TRACE includes bind args.
        .sqlx_logging_level(log::LevelFilter::Trace);
    Database::connect(db_options).await.ok()
}
