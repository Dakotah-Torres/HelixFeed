use duckdb::Connection;
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH}; 



const MIGRATIONS: &[(&str, &str)] = &[
    ("0001_create_migrations_table.sql", include_str!("../migrations/0001_create_migrations_table.sql")),
]; 

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

    let applied = get_applied_versions(conn)?;

    for (filename, sql) in MIGRATIONS {
        if applied.contains(*filename){
            continue; 
        }

        let tx = conn.transaction()?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        tx.execute_batch(sql)?;
        tx.execute("INSERT INTO _migrations (version, applied_at) VALUES (?, ?)", duckdb::params![filename, now])?;
        tx.commit()?
    }
    Ok(())

}