use duckdb::Connection;
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH}; 
use crate::logging::sys_logger::SysLogger;
use crate::logging::LogType;

const LOG_PATH: &str  = "logs/sys_logs.log";
const SYSTEM: &str = "DuckDB Migrations";

const MIGRATIONS: &[(&str, &str)] = &[
    ("0001_create_migrations_table.sql", include_str!("./duckdb_migrations/0001_create_migrations_table.sql")),
    ("0002_create_trades_normalized.sql", include_str!("./duckdb_migrations/0002_create_trades_normalized.sql"))
]; 

fn table_exists(conn: &Connection, name: &str) -> Result<bool, anyhow::Error> {
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?")?;
    let count: i64 = stmt.query_row(duckdb::params![name], |row| row.get(0))?;
    Ok(count > 0)
}

fn migration_init(conn: &mut Connection) -> Result<(), anyhow::Error> {
    if table_exists(conn, "_migrations")? {
        return Ok(());
    }

    let tx = conn.transaction()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
    tx.execute_batch(MIGRATIONS[0].1)?;
    tx.execute("INSERT INTO _migrations (version, applied_at) VALUES (?, ?)", duckdb::params![MIGRATIONS[0].0, now])?;
    tx.commit()?;
    Ok(())
}

fn get_applied_versions(conn: &Connection) -> Result<HashSet<String>, anyhow::Error> {
    
    let mut stmt = conn.prepare("SELECT version FROM _migrations")?; 
    let rows = stmt.query_map([],  |row| row.get::<_, String>(0))?; 
    let mut applied = HashSet::new(); 
    for r in rows {
        applied.insert(r?);
    }

    Ok(applied)
}


pub fn run_migrations(conn: &mut Connection) -> Result<(), anyhow::Error> {
    let mut db_log  = SysLogger::new(LOG_PATH.to_string(),SYSTEM.to_string())?;
    db_log.sys_log(LogType::Debug, "initiating migrations");

    migration_init(conn)?; 
    db_log.sys_log(LogType::Debug, "_migrations Initi check compleated");

    let applied = get_applied_versions(conn)?;
    for (filename, sql) in MIGRATIONS {
        if applied.contains(*filename){
            continue; 
        }

        let tx = conn.transaction()?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        tx.execute_batch(sql)?;
        tx.execute("INSERT INTO _migrations (version, applied_at) VALUES (?, ?)", duckdb::params![filename, now])?;
        tx.commit()?;
        db_log.sys_log(LogType::Info, "all migrations have been commited");
    }
    Ok(())

}


#[cfg(test)]
mod tests {
    use super::*; 

    #[test]
    fn test_migrations_apply_cleanly() {
        let mut conn = Connection::open_in_memory().unwrap();

        run_migrations(&mut conn).unwrap();
        let applied_once = get_applied_versions(&conn).unwrap();

        run_migrations(&mut conn).unwrap();
        let applied_twice = get_applied_versions(&conn).unwrap();

        assert_eq!(applied_once, applied_twice); 

    }
}