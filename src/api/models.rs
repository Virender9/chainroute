use std::sync::Arc;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response as AxumResponse};
use serde::Serialize;
use crate::api::AppState;

#[derive(Debug, Serialize)]
pub struct ModelListResponse {
    pub object: String, // "list"
    pub data: Vec<ModelEntry>,
}

#[derive(Debug, Serialize)]
pub struct ModelEntry {
    pub id: String,
    pub object: String, // "model"
    pub created: u64,
    pub owned_by: String,
}

pub async fn handle_list_models(
    State(state): State<Arc<AppState>>,
) -> AxumResponse {
    let current_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let config = state.config.load();
    let mut models = Vec::new();
    for alias in config.routes.keys() {
        models.push(ModelEntry {
            id: alias.clone(),
            object: "model".to_string(),
            created: current_timestamp,
            owned_by: "gateway-core".to_string(),
        });
    }

    // Sort models by id for consistent output
    models.sort_by(|a, b| a.id.cmp(&b.id));

    let response = ModelListResponse {
        object: "list".to_string(),
        data: models,
    };

    (StatusCode::OK, Json(response)).into_response()
}
