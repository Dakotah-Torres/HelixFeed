use duckdb::{Connection, Result};
use crate::config::Mode;
use crate::config::FeedType;
use crate::config::Feeds;




pub struct HelixDb {
    feed: Feeds,
    conn: Connection,
}

impl HelixDb {
    pub fn new(feed:Feeds ) -> Result<Self, anyhow::Error> {
        let conn = Connection::open(feed.db_location)?;
        Ok(HelixDb { feed, conn })
    }

    pub fn ensure_table(&self) -> Result<(), anyhow::Error> {
        let clean_symbol =  replace("/", "");

        let table_name = format!("{}__{}__{:?}__{:?}", self.feed.provider, clean_symbol, feed_type, mode);

        match mode {

            Mode::Raw => {
                let sql = format!("
                    CREATE TABLE IF NOT EXISTS {} (
                        id BIGINT PRIMARY KEY,
                        recived_at BIGINT NOT NULL,
                        raw_json VARCHAR NOT NULL
                        )
                    ", table_name); 
                self.conn.execute_batch(&sql)?;
                Ok(())
            }, 

            Mode::Normalized => {
                match feed_type {
                    FeedType::Trades => {
                        let sql =  format!("
                            CREATE TABLE IF NOT EXISTS {} (
                                id BIGINT PRIMARY KEY, 
                                recived_at BIGINT,
                                trade_timestamp BIGINT,
                                asset VARCHAR,
                                price BIGINT,
                                size BIGINT,
                                side VARCHAR
                            )
                        ", table_name);
                        self.conn.execute_batch(&sql)?;
                    }

                    FeedType::Ticks => {
                        let sql =  format!("
                            CREATE TABLE IF NOT EXISTS {} (
                                id BIGINT PRIMARY KEY,
                                received_at BIGINT,
                                tick_timestamp BIGINT,
                                asset VARCHAR,
                                price BIGINT
                            )
                        ", table_name);
                        self.conn.execute_batch(&sql)?;
                    }

                    FeedType::Book => {
                        let sql =  format!("
                            CREATE TABLE IF NOT EXISTS {} (
                                id BIGINT PRIMARY KEY,
                                received_at BIGINT,
                                book_timestamp BIGINT,
                                asset VARCHAR,
                                depth INTEGER,
                                bids VARCHAR,
                                asks VARCHAR
                            )
                        ", table_name);
                        self.conn.execute_batch(&sql)?;
                    }

                    _ => {} 
                    
                }

                Ok(())
            }
        }

    }

}