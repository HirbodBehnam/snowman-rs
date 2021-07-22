use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::mysql::{MySqlPoolOptions, MySqlQueryResult};
use sqlx::{Connection, MySql, Pool, Transaction};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;

type UserIdType = u32;

#[derive(Serialize, Default)]
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

/// Checks an Result and rolls back a transaction and returns from function if the result is Err
///
/// # Arguments
/// First argument must be transaction and the second one must be result
///
/// # Example
///
/// ```
/// let mut tx = conn.begin().await?;
/// try_rollback!(tx, Database::my_function(&mut tx, id).await);
/// ```
macro_rules! try_rollback {
    ($tx:expr,$result:expr) => {{
        if let Err(err) = $result {
            let _ = $tx.rollback().await;
            return Err(err.into());
        }
    }};
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
        sqlx::query("INSERT INTO `past_balance` (`user_id`,`balances`,`changed`) SELECT `user_id`, `balances`, UNIX_TIMESTAMP() FROM `current_balance` WHERE `user_id`=?")
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

    ///
    ///
    /// # Arguments
    ///
    /// * `id`:
    ///
    /// returns: Result<HashMap<String, Balances, RandomState>, Error>
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    #[lock_master::locker(id)]
    pub async fn get_balances(&self, id: UserIdType) -> Result<HashMap<String, Balances>> {
        // Get the balance with a connection
        let mut conn = self.pool.acquire().await?;
        let result = read_balances!(&mut conn, id)?;
        Ok(result)
    }

    #[lock_master::locker(id)]
    pub async fn get_past_balance(&self, id: UserIdType, time: u64) -> Result<HashMap<String, Balances>> {
        // Get the balance with a connection
        let mut conn = self.pool.acquire().await?;
        // Get current time and check if the time provided is after now
        if SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).expect("IM-FUCKING-POSSIBLE").as_secs() <= time {
            return read_balances!(&mut conn, id);
        }
        // Get the balance
        let db_balances_json: sqlx::Result<(String,)> =
            sqlx::query_as("SELECT `balances` FROM `past_balance` WHERE `user_id`=? AND `changed` <= ?")
                .bind(id)
                .bind(time)
                .fetch_one(&mut conn)
                .await;
        if let Err(err) = db_balances_json {
            return if let sqlx::Error::RowNotFound = err {
                Ok(HashMap::new())
            } else {
                Err(err.into())
            }
        }
        let db_balances_json = db_balances_json.unwrap();
        let db_balances_raw =
            serde_json::from_str::<HashMap<String, DatabaseBalances>>(db_balances_json.0.as_str())?;
        // Convert the data
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
    }

    #[lock_master::locker(id)]
    pub async fn add_free_balance(&self, id: UserIdType, currency: String, volume: i64) -> Result<()> {
        // Start a transaction
        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin().await?;
        // Move balance to past
        try_rollback!(tx, Database::move_to_past(&mut tx, id).await);
        // Add free balance
        try_rollback!(tx, Database::edit_current_balance(&mut tx, id, currency, volume, volume).await);
        tx.commit().await?;
        Ok(())
    }

    #[lock_master::locker(id)]
    pub async fn block_free_balance(&self, id: UserIdType, currency: String, volume: i64) -> Result<()> {
        // Start a transaction
        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin().await?;
        // Move balance to past
        try_rollback!(tx, Database::move_to_past(&mut tx, id).await);
        // Block balance by only removing free balance
        try_rollback!(tx, Database::edit_current_balance(&mut tx, id, currency, -volume, 0).await);
        tx.commit().await?;
        Ok(())
    }

    #[lock_master::locker(id)]
    pub async fn unblock_blocked_balance(&self, id: UserIdType, currency: String, volume: i64) -> Result<()> {
        // Start a transaction
        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin().await?;
        // Move balance to past
        try_rollback!(tx, Database::move_to_past(&mut tx, id).await);
        // Block balance by only adding free balance
        try_rollback!(tx, Database::edit_current_balance(&mut tx, id, currency, volume, 0).await);
        tx.commit().await?;
        Ok(())
    }

    #[lock_master::locker(id)]
    pub async fn withdraw_blocked_balance(&self, id: UserIdType, currency: String, volume: i64) -> Result<()> {
        // Start a transaction
        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin().await?;
        // Move balance to past
        try_rollback!(tx, Database::move_to_past(&mut tx, id).await);
        // Just remove from total
        try_rollback!(tx, Database::edit_current_balance(&mut tx, id, currency, 0, -volume).await);
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
