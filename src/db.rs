use std::env;
use tracing::{info, warn};

// --- CONDITIONAL TYPE ALIASING ---
#[cfg(feature = "sqlite")]
pub type DbPool = sqlx::SqlitePool;

#[cfg(feature = "postgres")]
pub type DbPool = sqlx::PgPool;

#[cfg(all(feature = "sqlite", feature = "postgres"))]
compile_error!("Feature 'sqlite' and 'postgres' cannot be enabled at the same time!");

#[cfg(not(any(feature = "sqlite", feature = "postgres")))]
compile_error!("You must enable either 'sqlite' or 'postgres' feature!");

// --- INIT POOL ---

pub async fn init_pool() -> Result<DbPool, Box<dyn std::error::Error>> {
    
    // LOGIC FIX: Handle DATABASE_URL parsing specific to the active feature
    // to prevent SQLite trying to parse a Postgres connection string from .env

    #[cfg(feature = "sqlite")]
    {
        use sqlx::{migrate::MigrateDatabase, sqlite::{SqlitePoolOptions, SqliteConnectOptions}, Sqlite};
        use std::str::FromStr;

        // Smart URL detection
        let db_url = match env::var("DATABASE_URL") {
            Ok(url) => {
                if url.starts_with("postgres://") || url.contains("sslmode=") {
                    warn!("⚠️  Detected PostgreSQL config in DATABASE_URL but running in SQLite mode.");
                    warn!("⚠️  Ignoring .env variable and defaulting to 'sqlite://zorp.db'");
                    "sqlite://zorp.db".to_string()
                } else {
                    url
                }
            },
            Err(_) => "sqlite://zorp.db".to_string(),
        };

        if !Sqlite::database_exists(&db_url).await.unwrap_or(false) {
            info!("Creating SQLite database at {}...", db_url);
            Sqlite::create_database(&db_url).await?;
        }

        let options = SqliteConnectOptions::from_str(&db_url)?
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

        return Ok(pool);
    }

    #[cfg(feature = "postgres")]
    {
        use sqlx::postgres::{PgPoolOptions, PgConnectOptions};
        use std::str::FromStr;
        use std::time::Duration;
        
        let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set for PostgreSQL feature");
        
        info!("Connecting to PostgreSQL database...");
        
        let mut connect_options = PgConnectOptions::from_str(&db_url)?;

        // PERFORMANCE TUNING
        connect_options = connect_options.options([
            ("synchronous_commit", "off"),
            ("application_name", "zorp-dispatcher"),
        ]);

        let pool = PgPoolOptions::new()
            .max_connections(50)
            .min_connections(10)
            .acquire_timeout(Duration::from_secs(60)) 
            .connect_with(connect_options).await?;

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
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            "#
        )
        .execute(&pool).await?;

        return Ok(pool);
    }
}

// --- QUERY HELPER ---
pub fn sql_placeholder(index: usize) -> String {
    #[cfg(feature = "sqlite")]
    {
        let _ = index;
        "?".to_string()
    }

    #[cfg(feature = "postgres")]
    {
        format!("${}", index)
    }
}