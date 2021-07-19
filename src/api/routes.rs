use serde::Deserialize;
use warp::{Filter, Reply};
use crate::db::db::Database;
use std::sync::Arc;
use crate::check_error;
use crate::api::errors::errors::from_error;
use std::net::SocketAddr;

/// Max request size in bytes
const MAX_REQUEST_SIZE: u64 = 1024;

#[derive(Deserialize)]
struct UserQuery {
    user_id: u32,
}

#[derive(Deserialize)]
struct ChangeBalanceRequest {
    sign: String,
    amount: i64,
    user_id: u32,
}

pub async fn run_server(listen_address: &str, database: Database) {
    let database = Arc::new(database);
    let database = warp::any().map(move || database.clone());
    let register = warp::put()
        .and(warp::path("register"))
        .and(warp::path::end())
        .and(warp::query::<UserQuery>())
        .and(database.clone())
        .and_then(register);
    let users_path = warp::path("users");
    let get_free_balance = users_path.and(warp::get()
        .and(warp::path::end())
        .and(warp::query::<UserQuery>())
        .and(database.clone())
        .and_then(get_free_balance));
    // Limit body
    let body_limiter = warp::body::content_length_limit(MAX_REQUEST_SIZE)
        .and(warp::body::json::<ChangeBalanceRequest>())
        .and(database.clone());
    let add_free_balance = users_path.and(warp::post()
        .and(warp::path("free"))
        .and(warp::path("add"))
        .and(warp::path::end())
        .and(body_limiter.clone())
        .and_then(add_free_balance));
    let withdraw_free_balance = users_path.and(warp::post()
        .and(warp::path("free"))
        .and(warp::path("withdraw"))
        .and(warp::path::end())
        .and(body_limiter.clone())
        .and_then(withdraw_free_balance));
    let block_free_balance = users_path.and(warp::post()
        .and(warp::path("free"))
        .and(warp::path("block"))
        .and(warp::path::end())
        .and(body_limiter.clone())
        .and_then(block_free_balance));
    let unblock_blocked_balance = users_path.and(warp::post()
        .and(warp::path("block"))
        .and(warp::path("unblock"))
        .and(warp::path::end())
        .and(body_limiter.clone())
        .and_then(unblock_blocked_balance));
    let withdraw_blocked_balance = users_path.and(warp::post()
        .and(warp::path("block"))
        .and(warp::path("withdraw"))
        .and(warp::path::end())
        .and(body_limiter.clone())
        .and_then(withdraw_blocked_balance));
    let final_routes = add_free_balance.or(register)
        .or(get_free_balance).or(withdraw_free_balance).or(block_free_balance)
        .or(unblock_blocked_balance).or(withdraw_blocked_balance);
    warp::serve(final_routes)
        .run(listen_address.parse::<SocketAddr>().expect("invalid listen address"))
        .await;
}

/// Tries to register a new user in database
///
/// # Arguments
///
/// * `user`: The user to register
/// * `db`: The database
///
/// returns: Result<Response<Body>, Rejection> This function always accepts the request
/// However, it fails with error body when the user already exists in database
///
async fn register(user: UserQuery, db: Arc<Database>) -> Result<warp::reply::Response, warp::Rejection> {
    let result = db.register_user(user.user_id).await;
    check_error!(result)
}

async fn get_free_balance(user: UserQuery, db: Arc<Database>) -> Result<warp::reply::Response, warp::Rejection> {
    let result = db.get_balances(user.user_id).await;
    check_error!(result)
}

async fn add_free_balance(
    request: ChangeBalanceRequest,
    db: Arc<Database>,
) -> Result<warp::reply::Response, warp::Rejection> {
    let result = db.add_free_balance(request.user_id, request.sign, request.amount).await;
    check_error!(result)
}

async fn withdraw_free_balance(
    request: ChangeBalanceRequest,
    db: Arc<Database>,
) -> Result<warp::reply::Response, warp::Rejection> {
    let result = db.add_free_balance(request.user_id, request.sign, -request.amount).await;
    check_error!(result)
}

async fn block_free_balance(
    request: ChangeBalanceRequest,
    db: Arc<Database>,
) -> Result<warp::reply::Response, warp::Rejection> {
    let result = db.block_free_balance(request.user_id, request.sign, -request.amount).await;
    check_error!(result)
}

async fn unblock_blocked_balance(
    request: ChangeBalanceRequest,
    db: Arc<Database>,
) -> Result<warp::reply::Response, warp::Rejection> {
    let result = db.unblock_blocked_balance(request.user_id, request.sign, -request.amount).await;
    check_error!(result)
}

async fn withdraw_blocked_balance(
    request: ChangeBalanceRequest,
    db: Arc<Database>,
) -> Result<warp::reply::Response, warp::Rejection> {
    let result = db.withdraw_blocked_balance(request.user_id, request.sign, -request.amount).await;
    check_error!(result)
}
