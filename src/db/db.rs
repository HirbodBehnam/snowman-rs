use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::mysql::{MySqlPoolOptions, MySqlQueryResult};
use sqlx::{Connection, MySql, Pool, Transaction};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

type UserIdType = u32;

#[derive(Serialize)]
pub struct Balances {
    pub free: i64,
    pub blocked: i64,
    pub total: i64,
}

#[derive(Deserialize, Serialize, Default)]
struct DatabaseBalances {
    free: i64,
    total: i64,
}

pub struct Database {
    pool: Pool<MySql>,
    lock_map: Mutex<HashMap<UserIdType, Arc<Mutex<()>>>>,
}

macro_rules! read_balances {
    ($executor:expr,$id:expr) => {{
        let ex = &mut *$executor; // this was the key! https://stackoverflow.com/a/30539264/4213397
        let db_balances_raw = read_raw_balances!(ex, $id)?;
        let mut result = HashMap::with_capacity(db_balances_raw.capacity());
        for (currency, data) in db_balances_raw {
            result.insert(
                currency,
                Balances {
                    free: data.free,
                    blocked: data.total - data.free,
                    total: data.total,
                },
            );
        }
        Ok(result)
    } as anyhow::Result<HashMap<String, Balances>>};
}

macro_rules! read_raw_balances {
    ($executor:expr,$id:expr) => {{
        let ex = &mut *$executor; // this was the key! https://stackoverflow.com/a/30539264/4213397
        let db_balances_json: (String,) =
            sqlx::query_as("SELECT `balances` FROM `current_balance` WHERE `user_id`=?")
                .bind($id)
                .fetch_one(ex)
                .await?;
        let db_balances_raw =
            serde_json::from_str::<HashMap<String, DatabaseBalances>>(db_balances_json.0.as_str())?;
        Ok(db_balances_raw)
    } as anyhow::Result<HashMap<String, DatabaseBalances>>};
}

impl Database {
    pub async fn new(uri: &str) -> Self {
        let pool = MySqlPoolOptions::new()
            .max_connections(150)
            .connect_timeout(Duration::from_secs(2))
            .connect(uri)
            .await
            .expect("cannot connect to database");
        Self {
            pool,
            lock_map: Mutex::new(HashMap::new()),
        }
    }

    async fn move_to_past(
        tx: &mut Transaction<'_, sqlx::MySql>,
        user_id: UserIdType,
    ) -> Result<MySqlQueryResult, sqlx::Error> {
        sqlx::query("INSERT INTO `past_balance` (`user_id`,`balances`,`changed`) SELECT `user_id`, `balances`, NOW() FROM `current_balance` WHERE `user_id`=?")
            .bind(user_id)
            .execute(tx)
            .await
    }

    /// Registers a new user in database
    /// Returns an error if the user already exists in database
    ///
    /// # Arguments
    ///
    /// * `id`: The user ID to register in database
    ///
    /// returns: Result<(), Error> Nothing on success, otherwise an error
    ///
    pub async fn register_user(&self, id: UserIdType) -> Result<()> {
        // Create a connection
        let mut conn = self.pool.acquire().await?;
        sqlx::query("INSERT INTO `current_balance` (`user_id`) VALUES (?)")
            .bind(id)
            .execute(&mut conn)
            .await?; // automatically returns an error if user_id is not unique
        Ok(())
    }

    #[lock_master::locker(id)]
    pub async fn get_balances(&self, id: UserIdType) -> Result<HashMap<String, Balances>> {
        // Get the balance with a connection
        let mut conn = self.pool.acquire().await?;
        let result = read_balances!(&mut conn, id)?;
        Ok(result)
    }

    #[lock_master::locker(id)]
    pub async fn add_free_balance(&self, id: UserIdType, currency: String, volume: i64) -> Result<()> {
        // Start a transaction
        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin().await?;
        // Move balance to past
        Database::move_to_past(&mut tx, id).await?; // note to myself: Transaction is rolled back if it goes out of scope
        // Add free balance
        Database::edit_current_balance(&mut tx, id, currency, volume, volume).await?;
        tx.commit().await?;
        Ok(())
    }

    #[lock_master::locker(id)]
    pub async fn block_free_balance(&self, id: UserIdType, currency: String, volume: i64) -> Result<()> {
        // Start a transaction
        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin().await?;
        // Move balance to past
        Database::move_to_past(&mut tx, id).await?; // note to myself: Transaction is rolled back if it goes out of scope
        // Block balance by only removing free balance
        Database::edit_current_balance(&mut tx, id, currency, -volume, 0).await?;
        tx.commit().await?;
        Ok(())
    }

    #[lock_master::locker(id)]
    pub async fn unblock_blocked_balance(&self, id: UserIdType, currency: String, volume: i64) -> Result<()> {
        // Start a transaction
        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin().await?;
        // Move balance to past
        Database::move_to_past(&mut tx, id).await?; // note to myself: Transaction is rolled back if it goes out of scope
        // Block balance by only adding free balance
        Database::edit_current_balance(&mut tx, id, currency, volume, 0).await?;
        tx.commit().await?;
        Ok(())
    }

    #[lock_master::locker(id)]
    pub async fn withdraw_blocked_balance(&self, id: UserIdType, currency: String, volume: i64) -> Result<()> {
        // Start a transaction
        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin().await?;
        // Move balance to past
        Database::move_to_past(&mut tx, id).await?; // note to myself: Transaction is rolled back if it goes out of scope
        // Just remove from total
        Database::edit_current_balance(&mut tx, id, currency, 0, -volume).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn edit_current_balance(tx: &mut Transaction<'_, sqlx::MySql>, id: UserIdType, currency: String, free_delta: i64, total_delta: i64) -> Result<()> {
        // Read old balance
        let mut balances = read_raw_balances!(tx, id)?;
        let mut balance = balances.entry(currency).or_default();
        // Change balance
        balance.total += total_delta;
        balance.free += free_delta;
        // Check balance
        if balance.free < 0 || balance.total < 0 || balance.total - balance.free < 0 {
            return Err(anyhow::Error::msg("insufficient balance"));
        }
        // Insert it into database
        sqlx::query("UPDATE `current_balance` SET `balances`=? WHERE `user_id`=?")
            .bind(serde_json::to_string(&balances).unwrap())
            .bind(id)
            .execute(tx)
            .await?;
        // Everything good
        Ok(())
    }
}
