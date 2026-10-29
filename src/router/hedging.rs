use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;
use crate::telemetry::metrics;

pub trait IsSuccess {
    fn is_success_status(&self) -> bool;
}

impl IsSuccess for reqwest::Response {
    fn is_success_status(&self) -> bool {
        self.status().as_u16() == 200
    }
}

impl IsSuccess for axum::response::Response {
    fn is_success_status(&self) -> bool {
        self.status().as_u16() == 200 || self.status().as_u16() == 400
    }
}

impl IsSuccess for () {
    fn is_success_status(&self) -> bool {
        true
    }
}

impl IsSuccess for String {
    fn is_success_status(&self) -> bool {
        true
    }
}

pub async fn execute_with_hedging<Fut, F, T, E>(
    primary_fut: Fut,
    secondary_provider: F,
    delay: Duration,
) -> Result<(T, &'static str), E>
where
    Fut: Future<Output = Result<T, E>>,
    F: FnOnce() -> Fut,
    T: IsSuccess,
{
    tokio::pin!(primary_fut);

    tokio::select! {
        res = &mut primary_fut => {
            match res {
                Ok(val) => Ok((val, "primary")),
                Err(e) => Err(e),
            }
        }
        _ = sleep(delay) => {
            metrics::inc_hedged_requests_total("");
            let secondary_fut = secondary_provider();
            tokio::pin!(secondary_fut);

            tokio::select! {
                res = &mut primary_fut => {
                    match res {
                        Ok(val) if val.is_success_status() => Ok((val, "primary")),
                        Ok(val) => {
                            let _ = val;
                            tokio::select! {
                                res_sec = &mut secondary_fut => {
                                    match res_sec {
                                        Ok(s_val) => Ok((s_val, "secondary")),
                                        Err(e) => Err(e),
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            match secondary_fut.await {
                                Ok(val) => Ok((val, "secondary")),
                                Err(e) => Err(e),
                            }
                        }
                    }
                }
                res = &mut secondary_fut => {
                    match res {
                        Ok(val) if val.is_success_status() => Ok((val, "secondary")),
                        Ok(val) => {
                            match primary_fut.await {
                                Ok(p_val) => Ok((p_val, "primary")),
                                Err(_) => Ok((val, "secondary")),
                            }
                        }
                        Err(_) => {
                            match primary_fut.await {
                                Ok(val) => Ok((val, "primary")),
                                Err(e) => Err(e),
                            }
                        }
                    }
                }
            }
        }
    }
}

