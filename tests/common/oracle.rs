#![allow(dead_code)]

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::header::AsHeaderName;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use tokio::io::AsyncWriteExt;
use tokio::process::Command as TokioCommand;

use super::*;

/// Locate git's `git-http-backend` CGI. It is shipped in git's core exec
/// directory (`git --exec-path`) rather than on `PATH`, so resolve it there.
fn git_http_backend_path() -> PathBuf {
    let output = Command::new("git")
        .arg("--exec-path")
        .output()
        .expect("run git --exec-path");
    assert!(
        output.status.success(),
        "git --exec-path failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let exec_path = String::from_utf8(output.stdout).expect("utf-8 exec-path");
    PathBuf::from(exec_path.trim()).join("git-http-backend")
}

/// Shared, immutable state every request handler reads: where bare repos live
/// and the CGI binary that serves them.
struct OracleState {
    /// `GIT_PROJECT_ROOT` (also the child's `HOME`, for hermeticity).
    root: PathBuf,
    /// Path to the resolved `git-http-backend` binary.
    backend: PathBuf,
}

/// A reference git server: an in-process HTTP listener whose every request is
/// answered by shelling out to git's own `git-http-backend` CGI. It serves the
/// bare repositories under its `GIT_PROJECT_ROOT` tempdir, so a differential
/// test can push to it and clone from it exactly as it would a real git host.
pub struct OracleServer {
    addr: SocketAddr,
    tempdir: TempDir,
    handle: JoinHandle<()>,
}

impl OracleServer {
    /// Bind `127.0.0.1:0` and serve `git-http-backend` in a background task.
    pub async fn spawn() -> OracleServer {
        let tempdir = TempDir::new().expect("create oracle tempdir");
        let state = Arc::new(OracleState {
            root: tempdir.path().to_path_buf(),
            backend: git_http_backend_path(),
        });
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("read listener addr");
        let app = Router::new().fallback(serve_cgi).with_state(state);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve oracle");
        });
        OracleServer {
            addr,
            tempdir,
            handle,
        }
    }

    /// Base URL of the running server, without a trailing slash.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Scratch directory (the `GIT_PROJECT_ROOT`) owned by this server.
    pub fn tempdir(&self) -> &Path {
        self.tempdir.path()
    }

    /// Create an empty bare repository `<root>/<name>.git` that accepts pushes
    /// over http (`http.receivepack=true`), and return its path. It is served
    /// at `{base_url}/{name}.git`. Its `HEAD` defaults to `main` (the branch
    /// the harness fixtures push to), so once `main` is pushed the server
    /// advertises a resolvable `HEAD` symref just as miscreant does.
    pub fn create_bare_repo(&self, name: &str) -> PathBuf {
        let repo = self.tempdir.path().join(format!("{name}.git"));
        let repo_str = repo.to_str().expect("utf-8 repo path");
        git_ok(
            self.tempdir.path(),
            &["init", "--bare", "-q", "-b", "main", repo_str],
        );
        git_ok(&repo, &["config", "http.receivepack", "true"]);
        repo
    }
}

impl Drop for OracleServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Answer one request by running `git-http-backend` as a CGI program: build
/// its CGI environment from the request, pipe the request body to its stdin,
/// and translate its CGI stdout back into an HTTP response.
async fn serve_cgi(State(state): State<Arc<OracleState>>, request: Request) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let path = uri.path().to_owned();
    let query = uri.query().unwrap_or_default().to_owned();
    let headers = request.headers();
    let content_type = header_str(headers, header::CONTENT_TYPE);
    let content_encoding = header_str(headers, header::CONTENT_ENCODING);
    // A CGI gateway forwards the client's `Git-Protocol` header so upload-pack
    // can negotiate protocol v2; without it the exchange silently falls back
    // to v0. `git-http-backend` honours an explicit `GIT_PROTOCOL` directly.
    let git_protocol = header_str(headers, "git-protocol");

    let body = match axum::body::to_bytes(request.into_body(), usize::MAX).await {
        Ok(body) => body,
        Err(err) => {
            return error_response(StatusCode::BAD_REQUEST, format!("read request body: {err}"));
        }
    };

    let mut command = TokioCommand::new(&state.backend);
    command
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("HOME", &state.root)
        .env("GIT_PROJECT_ROOT", &state.root)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("REQUEST_METHOD", method.as_str())
        .env("PATH_INFO", &path)
        .env("QUERY_STRING", &query)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(content_type) = &content_type {
        command.env("CONTENT_TYPE", content_type);
    }
    if method == Method::POST {
        command.env("CONTENT_LENGTH", body.len().to_string());
    }
    // `git-http-backend` reads the request's content encoding under the
    // CGI-standard `HTTP_` prefix and inflates a gzipped body itself.
    if let Some(content_encoding) = &content_encoding {
        command.env("HTTP_CONTENT_ENCODING", content_encoding);
    }
    if let Some(git_protocol) = &git_protocol {
        command.env("GIT_PROTOCOL", git_protocol);
    }

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("spawn git-http-backend: {err}"),
            );
        }
    };

    // Feed the body concurrently with draining stdout so neither side blocks
    // on a full pipe; dropping stdin closes it and signals EOF to the child.
    let mut stdin = child.stdin.take().expect("child stdin");
    let writer = tokio::spawn(async move {
        let _ = stdin.write_all(&body).await;
    });
    let output = match child.wait_with_output().await {
        Ok(output) => output,
        Err(err) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("wait for git-http-backend: {err}"),
            );
        }
    };
    let _ = writer.await;

    if !output.status.success() {
        let mut message = format!("git-http-backend exited {:?}\n", output.status.code());
        message.push_str(&String::from_utf8_lossy(&output.stderr));
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, message);
    }

    cgi_to_response(&output.stdout)
}

/// Read one request header as an owned string, if present and valid UTF-8.
fn header_str(headers: &HeaderMap, name: impl AsHeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

/// A short `text/plain` error response carrying `message`, so a failing CGI
/// invocation surfaces its cause instead of an opaque status.
fn error_response(status: StatusCode, message: String) -> Response {
    (status, message).into_response()
}

/// Translate a CGI program's stdout into an HTTP response: parse the header
/// block, map a `Status:` header to the response status (default 200), copy
/// the remaining headers, and use the bytes after the blank line as the body.
fn cgi_to_response(stdout: &[u8]) -> Response {
    let (header_bytes, body_bytes) = split_cgi(stdout);
    let mut status = StatusCode::OK;
    let mut builder = Response::builder();
    let header_text = String::from_utf8_lossy(header_bytes);
    for line in header_text.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim();
        let value = value.trim();
        if name.eq_ignore_ascii_case("Status") {
            if let Some(code) = value
                .split_whitespace()
                .next()
                .and_then(|code| code.parse::<u16>().ok())
                .and_then(|code| StatusCode::from_u16(code).ok())
            {
                status = code;
            }
            continue;
        }
        // Let the body's actual length drive framing rather than trusting the
        // CGI's own length/encoding headers.
        if name.eq_ignore_ascii_case("Content-Length")
            || name.eq_ignore_ascii_case("Transfer-Encoding")
        {
            continue;
        }
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            builder = builder.header(name, value);
        }
    }
    builder
        .status(status)
        .body(Body::from(body_bytes.to_vec()))
        .expect("build cgi response")
}

/// Split CGI output into its header block and body at the first blank line,
/// tolerating both `\r\n\r\n` and `\n\n` separators. With no blank line the
/// whole output is treated as the body.
fn split_cgi(output: &[u8]) -> (&[u8], &[u8]) {
    if let Some(pos) = find_subslice(output, b"\r\n\r\n") {
        (&output[..pos], &output[pos + 4..])
    } else if let Some(pos) = find_subslice(output, b"\n\n") {
        (&output[..pos], &output[pos + 2..])
    } else {
        (&[], output)
    }
}

/// The index of the first occurrence of `needle` in `haystack`, if any.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Assert two client invocations agreed on success/failure and exit code.
pub fn assert_same_exit(a: &Output, b: &Output) {
    assert_eq!(
        a.status.success(),
        b.status.success(),
        "exit success differs: {:?} vs {:?}",
        a.status,
        b.status
    );
    assert_eq!(
        a.status.code(),
        b.status.code(),
        "exit code differs: {:?} vs {:?}",
        a.status.code(),
        b.status.code()
    );
}

/// Assert `git ls-remote` (run from `dir`) advertises the same refs for both
/// URLs, compared as a sorted set since `ls-remote` does not order its output.
pub fn assert_same_ls_remote(dir: &Path, url_a: &str, url_b: &str) {
    let a = sorted_lines(&git_ok(dir, &["ls-remote", url_a]));
    let b = sorted_lines(&git_ok(dir, &["ls-remote", url_b]));
    assert_eq!(a, b, "ls-remote differs between {url_a} and {url_b}");
}

/// Assert both clones hold the same set of reachable objects
/// (`git rev-list --objects --all`), compared as a sorted set of oids.
pub fn assert_same_object_set(clone_a: &Path, clone_b: &Path) {
    assert_eq!(
        object_oids(clone_a),
        object_oids(clone_b),
        "reachable object sets differ"
    );
}

/// Assert both clones report the same reachable object count.
pub fn assert_same_object_count(clone_a: &Path, clone_b: &Path) {
    let count = |dir: &Path| {
        String::from_utf8(git_ok(dir, &["rev-list", "--count", "--objects", "--all"]))
            .expect("utf-8 rev-list count")
            .trim()
            .to_owned()
    };
    assert_eq!(count(clone_a), count(clone_b), "object counts differ");
}

/// Assert both clones carry the same local refs (name and target), compared as
/// a sorted set.
pub fn assert_same_refs(clone_a: &Path, clone_b: &Path) {
    assert_eq!(refs(clone_a), refs(clone_b), "local ref sets differ");
}

/// The sorted set of reachable object ids in `dir` (the oid is the first field
/// of each `git rev-list --objects --all` line; a trailing path is dropped).
fn object_oids(dir: &Path) -> Vec<String> {
    let output = git_ok(dir, &["rev-list", "--objects", "--all"]);
    let mut oids: Vec<String> = String::from_utf8(output)
        .expect("utf-8 rev-list output")
        .lines()
        .filter_map(|line| line.split_whitespace().next().map(str::to_owned))
        .collect();
    oids.sort();
    oids
}

/// The sorted set of `<oid> <refname>` lines for every local ref in `dir`.
fn refs(dir: &Path) -> Vec<String> {
    sorted_lines(&git_ok(
        dir,
        &["for-each-ref", "--format=%(objectname) %(refname)"],
    ))
}

/// Split `bytes` into lines and return them sorted, for order-insensitive
/// set comparison.
fn sorted_lines(bytes: &[u8]) -> Vec<String> {
    let mut lines: Vec<String> = String::from_utf8(bytes.to_vec())
        .expect("utf-8 git output")
        .lines()
        .map(str::to_owned)
        .collect();
    lines.sort();
    lines
}
