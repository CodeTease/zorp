use sqlx::{migrate::MigrateDatabase, sqlite::{SqlitePoolOptions, SqliteConnectOptions}, Sqlite, SqlitePool, ConnectOptions};
use std::str::FromStr;
use tracing::info;

pub const DB_URL: &str = "sqlite://zorp.db";

pub async fn init_pool() -> Result<SqlitePool, Box<dyn std::error::Error>> {
    if !Sqlite::database_exists(DB_URL).await.unwrap_or(false) {
        info!("Creating database: {}", DB_URL);
        Sqlite::create_database(DB_URL).await?;
    }

    // OPTIMIZE SQLITE FOR CONCURRENCY
    // Enable Write-Ahead Logging (WAL) to prevent database locking issues during high concurrency.
    let options = SqliteConnectOptions::from_str(DB_URL)?
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal) 
        .busy_timeout(std::time::Duration::from_secs(5))
        .foreign_keys(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(50)
        .connect_with(options).await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            exit_code INTEGER,
            image TEXT NOT NULL,
            commands TEXT NOT NULL,
            logs TEXT,
            callback_url TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        "#
    )
    .execute(&pool).await?;

    Ok(pool)
}