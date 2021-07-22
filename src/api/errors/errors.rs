use serde::Serialize;
use warp::Reply;

#[derive(Serialize)]
struct ErrorMessage {
    error: String,
}

#[macro_export]
macro_rules! check_error {
    ($result:expr) => {
        match $result {
            Ok(res) => {
                return Ok(warp::reply::json(&res).into_response());
            },
            Err(e) => {
                return Ok(from_error(e));
            }
        }
    };
    ($result:expr, ()) => {
        match $result {
            Ok(_) => {
                return Ok(empty_json());
            },
            Err(e) => {
                return Ok(from_error(e));
            }
        }
    };
}

#[inline]
pub fn from_error(err: anyhow::Error) -> warp::reply::Response {
    error_message(err.to_string(), 500)
}

#[inline]
fn error_message(message: String, status: u16) -> warp::reply::Response {
    warp::http::Response::builder()
        .status(status)
        .header(warp::http::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_string(&ErrorMessage {
            error: message,
        }).unwrap())
        .into_response()
}

#[inline]
pub fn empty_json() -> warp::reply::Response {
    warp::http::Response::builder()
        .header(warp::http::header::CONTENT_TYPE, "application/json")
        .body("{}")
        .into_response()
}