use anyhow::{Context, Result};
use std::env;

#[derive(Clone, Debug)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub r2_account_id: String,
    pub r2_access_key_id: String,
    pub r2_secret_access_key: String,
    pub r2_bucket_name: String,
    pub r2_public_url: String,
    pub callback_base_url: String,
    pub worker_concurrency: usize,
    pub max_job_attempts: i32,
    pub temp_dir: String,
    pub database_url: String,
    pub admin_username: String,
    pub admin_password: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            host: env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            port: env::var("PORT")
                .unwrap_or_else(|_| "3000".to_string())
                .parse()
                .context("Invalid PORT")?,
            r2_account_id: env::var("R2_ACCOUNT_ID").context("R2_ACCOUNT_ID required")?,
            r2_access_key_id: env::var("R2_ACCESS_KEY_ID")
                .context("R2_ACCESS_KEY_ID required")?,
            r2_secret_access_key: env::var("R2_SECRET_ACCESS_KEY")
                .context("R2_SECRET_ACCESS_KEY required")?,
            r2_bucket_name: env::var("R2_BUCKET_NAME").context("R2_BUCKET_NAME required")?,
            r2_public_url: env::var("R2_PUBLIC_URL").context("R2_PUBLIC_URL required")?,
            callback_base_url: env::var("CALLBACK_BASE_URL").unwrap_or_default(),
            worker_concurrency: env::var("WORKER_CONCURRENCY")
                .unwrap_or_else(|_| "2".to_string())
                .parse()
                .context("Invalid WORKER_CONCURRENCY")?,
            max_job_attempts: env::var("MAX_JOB_ATTEMPTS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .context("Invalid MAX_JOB_ATTEMPTS")?,
            temp_dir: env::var("TEMP_DIR").unwrap_or_else(|_| "/tmp/transcoder".to_string()),
            database_url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| "sqlite:transcoder.db".to_string()),
            admin_username: env::var("ADMIN_USERNAME")
                .unwrap_or_else(|_| "admin".to_string()),
            admin_password: env::var("ADMIN_PASSWORD")
                .context("ADMIN_PASSWORD is required")?,
        })
    }

    pub fn r2_endpoint(&self) -> String {
        format!(
            "https://{}.r2.cloudflarestorage.com",
            self.r2_account_id
        )
    }
}
