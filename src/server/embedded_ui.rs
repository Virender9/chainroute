use axum::response::{IntoResponse, Response};
use axum::http::Uri;
use mime_guess::mime;
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "ui/"]
pub struct UiAssets;

pub async fn serve_embedded_ui(uri: Uri) -> impl IntoResponse {
    let path = uri.path();

    // Serve index.html for root and /dashboard
    let asset_path = if path == "/" || path == "/dashboard" {
        "index.html"
    } else {
        // Strip leading slash for asset lookup
        path.strip_prefix('/').unwrap_or(path)
    };

    match UiAssets::get(asset_path) {
        Some(content) => {
            let mime_type = mime_guess::from_path(asset_path)
                .first_or_octet_stream();
            let mut response = Response::new(content.data.into());
            response.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                mime_type.as_ref().parse().unwrap_or(mime::TEXT_HTML_UTF_8.as_ref().parse().unwrap()),
            );
            response
        }
        None => {
            // Fall back to index.html for SPA routing
            match UiAssets::get("index.html") {
                Some(content) => {
                    let mut response = Response::new(content.data.into());
                    response.headers_mut().insert(
                        axum::http::header::CONTENT_TYPE,
                        mime::TEXT_HTML_UTF_8.as_ref().parse().unwrap(),
                    );
                    response
                }
                None => axum::http::StatusCode::NOT_FOUND.into_response(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[tokio::test]
    async fn test_serve_embedded_ui_dashboard() {
        let uri: Uri = "/dashboard".parse().unwrap();
        let resp = serve_embedded_ui(uri).await.into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(axum::http::header::CONTENT_TYPE).unwrap().to_str().unwrap().starts_with("text/html"),
            true
        );

        let uri_root: Uri = "/".parse().unwrap();
        let resp_root = serve_embedded_ui(uri_root).await.into_response();
        assert_eq!(resp_root.status(), StatusCode::OK);
    }
}