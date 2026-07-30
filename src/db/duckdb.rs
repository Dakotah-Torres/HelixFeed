use duckdb::{Connection, Result};
use crate::config::ProviderConfig;
use crate::db::duckdb_migrations::run_migrations;


pub struct HelixDb {
    feed: ProviderConfig,
    conn: Connection,
}

impl HelixDb {
    pub fn new(feed:ProviderConfig ) -> Result<Self, anyhow::Error> {
        let conn = Connection::open(feed.clone().db_location)?;
        Ok(HelixDb { feed, conn })
    }

    pub fn new_with_migration(feed: ProviderConfig) -> Result<Self, anyhow::Error> {
        let mut db = HelixDb::new(feed)?;
        run_migrations(&mut db.conn)?;
        Ok(db)
    }
}