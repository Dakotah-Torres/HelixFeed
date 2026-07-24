use duckdb::{Connection, Result};
use crate::config::Feeds;
use crate::db::duckdb_migrations::run_migrations;


pub struct HelixDb {
    feed: Feeds,
    conn: Connection,
}

impl HelixDb {
    pub fn new(feed:Feeds ) -> Result<Self, anyhow::Error> {
        let conn = Connection::open(feed.clone().db_location)?;
        Ok(HelixDb { feed, conn })
    }

    pub fn new_with_migration(feed: Feeds) -> Result<Self, anyhow::Error> {
        let mut db = HelixDb::new(feed)?;
        run_migrations(&mut db.conn)?;
        Ok(db)
    }
}