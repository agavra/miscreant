//! HTTP surface for the smart-git protocol.
//!
//! See `docs/0001-init.md` §Transfer Protocol (path scheme, v2-only fetch),
//! §Fetch API (capability list), and §Receive API (discovery). Repositories
//! are addressed by the request path minus the endpoint suffix. The push RPC
//! is served by `receive_pack`; the fetch RPC by `upload_pack`.

use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use tracing::Instrument;

use crate::AppState;
use crate::protocol::{advertise, receive_pack, upload_pack, upload_pack_v0};

/// Endpoint suffix for a ref-advertisement request.
const INFO_REFS: &str = "info/refs";
/// Endpoint suffix for the fetch (upload-pack) RPC.
const UPLOAD_PACK: &str = "git-upload-pack";
/// Endpoint suffix for the push (receive-pack) RPC.
const RECEIVE_PACK: &str = "git-receive-pack";

/// Service value selecting the fetch advertisement.
const SERVICE_UPLOAD_PACK: &str = "git-upload-pack";
/// Service value selecting the push advertisement.
const SERVICE_RECEIVE_PACK: &str = "git-receive-pack";

/// `GET /<repo>/info/refs?service=…` — the ref/service advertisement.
///
/// `git` addresses repositories with arbitrary depth (`org/repo`), so the
/// route is a single trailing wildcard and the endpoint suffix is stripped
/// here rather than matched by the router (a catch-all must be the last path
/// segment).
pub async fn info_refs(
    State(state): State<AppState>,
    Path(path): Path<String>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let Some(prefix) = strip_endpoint(&path, INFO_REFS) else {
        return plain(StatusCode::NOT_FOUND, "not found");
    };
    let repo = match repo_name(prefix) {
        Ok(name) => name,
        Err(RepoNameError) => return plain(StatusCode::BAD_REQUEST, "invalid repository name"),
    };

    match service_param(query.as_deref()).as_deref() {
        Some(SERVICE_UPLOAD_PACK) => {
            let span = tracing::debug_span!("advertise", repo = %repo, endpoint = "info/refs");
            upload_pack_advert(&state, &repo, &headers)
                .instrument(span)
                .await
        }
        Some(SERVICE_RECEIVE_PACK) => {
            let span = tracing::debug_span!("advertise", repo = %repo, endpoint = "info/refs");
            receive_pack_advert(&state, &repo).instrument(span).await
        }
        _ => plain(
            StatusCode::BAD_REQUEST,
            "missing or unsupported service parameter",
        ),
    }
}

/// `POST /<repo>/git-upload-pack` and `POST /<repo>/git-receive-pack` — the
/// push and fetch RPCs. Each strips its endpoint suffix to recover the
/// repository name and hands off to the matching handler.
pub async fn git_rpc(
    State(state): State<AppState>,
    Path(path): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(prefix) = strip_endpoint(&path, RECEIVE_PACK) {
        let repo = match repo_name(prefix) {
            Ok(name) => name,
            Err(RepoNameError) => return plain(StatusCode::BAD_REQUEST, "invalid repository name"),
        };
        let span = tracing::debug_span!("receive_pack", repo = %repo, endpoint = "receive-pack");
        receive_pack::receive_pack(&state, &repo, body)
            .instrument(span)
            .await
    } else if let Some(prefix) = strip_endpoint(&path, UPLOAD_PACK) {
        let repo = match repo_name(prefix) {
            Ok(name) => name,
            Err(RepoNameError) => return plain(StatusCode::BAD_REQUEST, "invalid repository name"),
        };
        let span = tracing::debug_span!("upload_pack", repo = %repo, endpoint = "upload-pack");
        // `git` sends the protocol-v2 header on the POST too when speaking v2,
        // so its presence selects the handler; its absence is the classic (v0)
        // protocol.
        if wants_v2(&headers) {
            upload_pack::upload_pack(&state, &repo, &headers, body)
                .instrument(span)
                .await
        } else {
            upload_pack_v0::upload_pack(&state, &repo, body)
                .instrument(span)
                .await
        }
    } else {
        plain(StatusCode::NOT_FOUND, "not found")
    }
}

/// Advertise upload-pack (fetch) capabilities. Protocol v2 gets the v2
/// capability advertisement; the classic (v0) protocol gets a ref
/// advertisement. Either way an unknown repository is a 404 (upload-pack never
/// auto-creates).
async fn upload_pack_advert(state: &AppState, repo: &str, headers: &HeaderMap) -> Response {
    let meta = match state.store.lookup_repo(repo).await {
        Ok(Some(meta)) => meta,
        Ok(None) => return plain(StatusCode::NOT_FOUND, "repository not found"),
        Err(_) => return internal_error(),
    };

    if wants_v2(headers) {
        let mut body = Vec::new();
        if advertise::upload_pack(&mut body).is_err() {
            return internal_error();
        }
        return git_response(
            StatusCode::OK,
            advertise::UPLOAD_ADVERTISEMENT_CONTENT_TYPE,
            body,
        );
    }

    match upload_pack_v0::advertise(state, &meta).await {
        Ok(body) => git_response(
            StatusCode::OK,
            advertise::UPLOAD_ADVERTISEMENT_CONTENT_TYPE,
            body,
        ),
        Err(()) => internal_error(),
    }
}

/// Advertise receive-pack (push) refs and capabilities. Auto-creates an
/// unknown repository when configured, otherwise 404s.
async fn receive_pack_advert(state: &AppState, repo: &str) -> Response {
    let meta = if state.config.auto_create_repos {
        match state.store.get_or_create_repo(repo).await {
            Ok(meta) => meta,
            Err(_) => return internal_error(),
        }
    } else {
        match state.store.lookup_repo(repo).await {
            Ok(Some(meta)) => meta,
            Ok(None) => return plain(StatusCode::NOT_FOUND, "repository not found"),
            Err(_) => return internal_error(),
        }
    };

    let refs = match state.store.list_refs(meta.id, None).await {
        Ok(refs) => refs,
        Err(_) => return internal_error(),
    };

    let mut body = Vec::new();
    if advertise::receive_pack(&mut body, &refs, meta.object_format).is_err() {
        return internal_error();
    }
    git_response(
        StatusCode::OK,
        advertise::RECEIVE_ADVERTISEMENT_CONTENT_TYPE,
        body,
    )
}

/// A rejected repository name (empty, or containing a traversal component).
#[derive(Debug)]
struct RepoNameError;

/// If `path` targets `endpoint` (`<repo>/<endpoint>` or just `<endpoint>`),
/// return the `<repo>` prefix (possibly empty); otherwise `None`.
fn strip_endpoint<'a>(path: &'a str, endpoint: &str) -> Option<&'a str> {
    if path == endpoint {
        Some("")
    } else {
        path.strip_suffix(endpoint)
            .and_then(|p| p.strip_suffix('/'))
    }
}

/// Resolve the repository name from the segment(s) preceding an endpoint.
/// Strips a single optional trailing `.git`; rejects an empty name and any
/// empty/`.`/`..` component (path traversal).
fn repo_name(prefix: &str) -> Result<String, RepoNameError> {
    let name = prefix.strip_suffix(".git").unwrap_or(prefix);
    if name.is_empty() {
        return Err(RepoNameError);
    }
    for component in name.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(RepoNameError);
        }
    }
    Ok(name.to_owned())
}

/// Extract the `service` value from a raw query string, if present.
fn service_param(query: Option<&str>) -> Option<String> {
    query?
        .split('&')
        .find_map(|pair| pair.strip_prefix("service=").map(|value| value.to_owned()))
}

/// Classify a request into the fixed `endpoint` label used by the HTTP
/// request metrics: `advert-upload`/`advert-receive` for `GET .../info/refs`
/// (distinguished by its `service` query parameter), or
/// `receive-pack`/`upload-pack` for the matching POST RPC. `None` for
/// anything else (`/healthz`, `/metrics`, an advert request with no
/// recognized `service`, or a path this server does not route at all) —
/// those are excluded from the metrics rather than forced into one of the
/// four labels, so the label's cardinality never grows no matter what a
/// client requests. `path` may carry a leading slash (as
/// `Uri::path()` returns it) or not.
pub(crate) fn classify_endpoint(path: &str, query: Option<&str>) -> Option<&'static str> {
    let path = path.strip_prefix('/').unwrap_or(path);
    if strip_endpoint(path, INFO_REFS).is_some() {
        return match service_param(query).as_deref() {
            Some(SERVICE_UPLOAD_PACK) => Some("advert-upload"),
            Some(SERVICE_RECEIVE_PACK) => Some("advert-receive"),
            _ => None,
        };
    }
    if strip_endpoint(path, RECEIVE_PACK).is_some() {
        return Some("receive-pack");
    }
    if strip_endpoint(path, UPLOAD_PACK).is_some() {
        return Some("upload-pack");
    }
    None
}

/// Whether the request opts into protocol v2 via the `Git-Protocol` header.
pub(crate) fn wants_v2(headers: &HeaderMap) -> bool {
    headers
        .get("git-protocol")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("version=2"))
}

/// Build a git response with the mandatory `Cache-Control: no-cache` header.
/// The body may be a buffer or a streaming `axum::body::Body`.
pub(crate) fn git_response(
    status: StatusCode,
    content_type: &'static str,
    body: impl IntoResponse,
) -> Response {
    (
        status,
        [
            (header::CONTENT_TYPE, HeaderValue::from_static(content_type)),
            (header::CACHE_CONTROL, HeaderValue::from_static("no-cache")),
        ],
        body,
    )
        .into_response()
}

/// A short `text/plain` git response (errors, stubs).
pub(crate) fn plain(status: StatusCode, message: &str) -> Response {
    git_response(
        status,
        "text/plain; charset=utf-8",
        message.as_bytes().to_vec(),
    )
}

/// A generic 500 for unexpected storage failures.
pub(crate) fn internal_error() -> Response {
    plain(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_strip_endpoint_from_nested_repo_path() {
        // given/when/then
        assert_eq!(
            strip_endpoint("org/repo.git/info/refs", INFO_REFS),
            Some("org/repo.git")
        );
        assert_eq!(
            strip_endpoint("repo/git-upload-pack", UPLOAD_PACK),
            Some("repo")
        );
    }

    #[test]
    fn should_treat_bare_endpoint_as_empty_repo_prefix() {
        // given/when/then
        assert_eq!(strip_endpoint("info/refs", INFO_REFS), Some(""));
    }

    #[test]
    fn should_not_match_endpoint_on_unrelated_path() {
        // given/when/then
        assert_eq!(strip_endpoint("repo/info/pack", INFO_REFS), None);
        assert_eq!(strip_endpoint("info/refs", UPLOAD_PACK), None);
        // A path segment merely ending in the endpoint text is not a match.
        assert_eq!(strip_endpoint("myinfo/refs", INFO_REFS), None);
    }

    #[test]
    fn should_strip_single_trailing_git_suffix() {
        // given/when/then
        assert_eq!(repo_name("org/repo.git").unwrap(), "org/repo");
        assert_eq!(repo_name("org/repo").unwrap(), "org/repo");
        // Only one trailing `.git` is removed.
        assert_eq!(repo_name("repo.git.git").unwrap(), "repo.git");
    }

    #[test]
    fn should_reject_empty_repo_name() {
        // given/when/then
        assert!(repo_name("").is_err());
        assert!(repo_name(".git").is_err());
    }

    #[test]
    fn should_reject_repo_names_with_traversal_components() {
        // given/when/then
        assert!(repo_name("..").is_err());
        assert!(repo_name("org/../secret").is_err());
        assert!(repo_name("./repo").is_err());
        // Empty components (leading/trailing/double slash) are rejected too.
        assert!(repo_name("org//repo").is_err());
        assert!(repo_name("/repo").is_err());
    }

    #[test]
    fn should_parse_service_query_parameter() {
        // given/when/then
        assert_eq!(
            service_param(Some("service=git-upload-pack")).as_deref(),
            Some("git-upload-pack")
        );
        assert_eq!(
            service_param(Some("foo=bar&service=git-receive-pack")).as_deref(),
            Some("git-receive-pack")
        );
        assert_eq!(service_param(Some("foo=bar")), None);
        assert_eq!(service_param(None), None);
    }

    #[test]
    fn should_classify_advert_and_rpc_endpoints_for_metrics() {
        // given/when/then: each of the four served paths yields its label
        assert_eq!(
            classify_endpoint("org/repo/info/refs", Some("service=git-upload-pack")),
            Some("advert-upload")
        );
        assert_eq!(
            classify_endpoint("/org/repo/info/refs", Some("service=git-receive-pack")),
            Some("advert-receive")
        );
        assert_eq!(
            classify_endpoint("repo/git-receive-pack", None),
            Some("receive-pack")
        );
        assert_eq!(
            classify_endpoint("repo/git-upload-pack", None),
            Some("upload-pack")
        );
    }

    #[test]
    fn should_not_classify_operational_or_unrecognized_paths() {
        // given/when/then: /healthz, /metrics, an advert with no recognized
        // service, and a path matching no endpoint at all are all excluded
        assert_eq!(classify_endpoint("healthz", None), None);
        assert_eq!(classify_endpoint("metrics", None), None);
        assert_eq!(classify_endpoint("repo/info/refs", None), None);
        assert_eq!(
            classify_endpoint("repo/info/refs", Some("service=other")),
            None
        );
        assert_eq!(classify_endpoint("repo/some-other-path", None), None);
    }

    #[test]
    fn should_detect_protocol_v2_header() {
        // given
        let mut with_v2 = HeaderMap::new();
        with_v2.insert("git-protocol", HeaderValue::from_static("version=2"));
        let mut without = HeaderMap::new();
        without.insert("git-protocol", HeaderValue::from_static("version=1"));

        // when/then
        assert!(wants_v2(&with_v2));
        assert!(!wants_v2(&without));
        assert!(!wants_v2(&HeaderMap::new()));
    }
}
