use duckdb::{Connection, Result};
use crate::config::DBConfig;
use crate::db::duckdb_migrations::run_migrations;


pub struct NormalizedDuckDB {
    conn: Connection,
}

impl NormalizedDuckDB {
    pub fn new(db_conf: DBConfig ) -> Result<Self, anyhow::Error> {
        let conn = Connection::open( db_conf.duckdb_location)?;
        Ok(NormalizedDuckDB { conn })
    }

    pub fn new_with_migration(db_conf: DBConfig) -> Result<Self, anyhow::Error> {
        let mut db = NormalizedDuckDB::new(db_conf)?;
        run_migrations(&mut db.conn)?;
        Ok(db)
    }
}