use std::env;
use tracing::info;

// --- CONDITIONAL TYPE ALIASING ---
// Priority: Postgres > Sqlite
// If both are enabled (e.g. default features + postgres), we use Postgres.

#[cfg(feature = "postgres")]
pub type DbPool = sqlx::PgPool;

#[cfg(all(feature = "sqlite", not(feature = "postgres")))]
pub type DbPool = sqlx::SqlitePool;

#[cfg(not(any(feature = "sqlite", feature = "postgres")))]
compile_error!("You must enable either 'sqlite' or 'postgres' feature!");

// --- INIT POOL ---

pub async fn init_pool() -> Result<DbPool, Box<dyn std::error::Error>> {
    
    // Postgres Path
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
            ("statement_timeout", "60000"), 
            ("idle_in_transaction_session_timeout", "60000"),
        ]);

        let max_conns = env::var("ZORP_DB_MAX_CONNS").ok().and_then(|v| v.parse().ok()).unwrap_or(50);
        let min_conns = env::var("ZORP_DB_MIN_CONNS").ok().and_then(|v| v.parse().ok()).unwrap_or(10);
        let timeout = env::var("ZORP_DB_TIMEOUT").ok().and_then(|v| v.parse().ok()).unwrap_or(30);

        info!("⚙️  DB Config (PG): Max={}, Min={}, Timeout={}s", max_conns, min_conns, timeout);

        let pool = PgPoolOptions::new()
            .max_connections(max_conns)
            .min_connections(min_conns)
            .max_lifetime(Duration::from_secs(30 * 60)) 
            .acquire_timeout(Duration::from_secs(timeout)) 
            .connect_with(connect_options).await?;

        // Run migrations
        info!("Running PostgreSQL migrations...");
        let migrator = sqlx::migrate::Migrator::new(std::path::Path::new("migrations/postgres")).await?;
        migrator.run(&pool).await?;

        return Ok(pool);
    }

    // Sqlite Path (Only if Postgres is NOT enabled)
    #[cfg(all(feature = "sqlite", not(feature = "postgres")))]
    {
        use sqlx::{migrate::MigrateDatabase, sqlite::{SqlitePoolOptions, SqliteConnectOptions}, Sqlite};
        use std::str::FromStr;
        use tracing::warn;

        // Warning for Production
        warn!("⚠️  WARNING: Running in SQLite mode. This is NOT recommended for production CI/CD workloads.");
        warn!("⚠️  Please use PostgreSQL for better concurrency and performance.");

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

        let max_conns = env::var("ZORP_DB_MAX_CONNS").ok().and_then(|v| v.parse().ok()).unwrap_or(50);
        info!("⚙️  DB Config (SQLite): Max={}", max_conns);

        let pool = SqlitePoolOptions::new()
            .max_connections(max_conns) 
            .connect_with(options).await?;

        // Run migrations
        info!("Running SQLite migrations...");
        let migrator = sqlx::migrate::Migrator::new(std::path::Path::new("migrations/sqlite")).await?;
        migrator.run(&pool).await?;

        return Ok(pool);
    }
}

