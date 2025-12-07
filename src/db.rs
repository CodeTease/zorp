use std::env;
use tracing::{info, warn};

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

        let pool = PgPoolOptions::new()
            .max_connections(50)
            .min_connections(10)
            .max_lifetime(Duration::from_secs(30 * 60)) 
            .acquire_timeout(Duration::from_secs(30)) 
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
                artifact_url TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            "#
        )
        .execute(&pool).await?;

        // Simple migration
        let _ = sqlx::query("ALTER TABLE jobs ADD COLUMN artifact_url TEXT").execute(&pool).await;

        return Ok(pool);
    }

    // Sqlite Path (Only if Postgres is NOT enabled)
    #[cfg(all(feature = "sqlite", not(feature = "postgres")))]
    {
        use sqlx::{migrate::MigrateDatabase, sqlite::{SqlitePoolOptions, SqliteConnectOptions}, Sqlite};
        use std::str::FromStr;

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
                artifact_url TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            "#
        )
        .execute(&pool).await?;

        // Simple migration
        let _ = sqlx::query("ALTER TABLE jobs ADD COLUMN artifact_url TEXT").execute(&pool).await;

        return Ok(pool);
    }
}

// --- QUERY HELPER ---
pub fn sql_placeholder(index: usize) -> String {
    #[cfg(feature = "postgres")]
    {
        format!("${}", index)
    }

    #[cfg(all(feature = "sqlite", not(feature = "postgres")))]
    {
        let _ = index;
        "?".to_string()
    }
}
