use crate::api::routes::run_server;
use crate::db::db::Database;
use std::env;

mod api;
mod db;

#[tokio::main]
async fn main() {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be specified");
    let listen_address = env::var("LISTEN").expect("LISTEN must be specified");
    let database = Database::new(database_url.as_str()).await;
    run_server(listen_address.as_str(), database).await;
}