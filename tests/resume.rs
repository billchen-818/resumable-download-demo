use std::{
    io,
    path::Path,
    process::{Output, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::Response,
    routing::get,
};
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use tokio::{net::TcpListener, process::Command, task::JoinHandle, time};

const V1: &str = "\"fixture-v1\"";
const V2: &str = "\"fixture-v2\"";

#[derive(Clone, Copy)]
enum Mode {
    Resume,
    Replace,
    Ignore,
    BadRange,
    WeakTag,
    Unsatisfiable,
    Hold,
}

type RequestHeaders = (Option<String>, Option<String>);

#[derive(Clone)]
struct Fixture {
    bytes: Bytes,
    mode: Mode,
    requests: Arc<Mutex<Vec<RequestHeaders>>>,
}

async fn serve(State(fixture): State<Fixture>, headers: HeaderMap) -> Response {
    let range = headers
        .get(header::RANGE)
        .map(|value| value.to_str().unwrap().to_owned());
    let if_range = headers
        .get(header::IF_RANGE)
        .map(|value| value.to_str().unwrap().to_owned());
    let request_index = {
        let mut seen = fixture.requests.lock().unwrap();
        seen.push((range.clone(), if_range.clone()));
        seen.len()
    };

    let mut bytes = fixture.bytes;
    let mut tag = V1;
    if request_index >= 2 && matches!(fixture.mode, Mode::Replace) {
        let mut replacement = bytes.to_vec();
        let last = replacement.len() - 1;
        replacement[0] ^= 1;
        replacement[last] ^= 1;
        bytes = Bytes::from(replacement);
        tag = V2;
    }
    if matches!(fixture.mode, Mode::WeakTag) {
        tag = "W/\"fixture-v1\"";
    }

    let total = bytes.len();
    let mut response = Response::builder()
        .header(header::ETAG, tag)
        .header(header::ACCEPT_RANGES, "bytes");
    if request_index == 1 {
        let prefix = bytes.slice(..total / 3);
        let stream = async_stream::stream! {
            yield Ok::<_, io::Error>(prefix);
            if matches!(fixture.mode, Mode::Hold) {
                std::future::pending::<()>().await;
            }
            time::sleep(Duration::from_millis(200)).await;
            yield Err(io::Error::other("fixture disconnected midway"));
        };
        return response
            .header(header::CONTENT_LENGTH, total)
            .body(Body::from_stream(stream))
            .unwrap();
    }

    if matches!(fixture.mode, Mode::Unsatisfiable) && range.is_some() {
        return response
            .status(StatusCode::RANGE_NOT_SATISFIABLE)
            .header(header::CONTENT_RANGE, format!("bytes */{total}"))
            .body(Body::empty())
            .unwrap();
    }

    if !matches!(fixture.mode, Mode::Ignore)
        && if_range.as_deref() == Some(tag)
        && let Some(range) = range
    {
        let offset: usize = range
            .strip_prefix("bytes=")
            .unwrap()
            .strip_suffix('-')
            .unwrap()
            .parse()
            .unwrap();
        let start = offset + usize::from(matches!(fixture.mode, Mode::BadRange));
        response = response.status(StatusCode::PARTIAL_CONTENT).header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{}/{total}", total - 1),
        );
        return response.body(Body::from(bytes.slice(start..))).unwrap();
    }

    response.body(Body::from(bytes)).unwrap()
}

async fn start(bytes: Bytes, mode: Mode) -> (String, Fixture, JoinHandle<()>) {
    let fixture = Fixture {
        bytes,
        mode,
        requests: Arc::default(),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/sample.mp4", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/sample.mp4", get(serve))
        .with_state(fixture.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, fixture, server)
}

fn command(url: &str, path: &Path, hash: &str) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_resumable-download-demo"));
    cmd.args([url, "-o"])
        .arg(path)
        .args(["--sha256", hash])
        .env("NO_PROXY", "127.0.0.1")
        .kill_on_drop(true);
    cmd
}

async fn download(url: &str, path: &Path, hash: &str) -> Output {
    time::timeout(Duration::from_secs(10), command(url, path, hash).output())
        .await
        .unwrap()
        .unwrap()
}

fn hash(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

async fn exercise(mode: Mode, bytes: Bytes, saved_video: Option<&Path>) {
    let dir = tempdir().unwrap();
    let output = dir.path().join("result.mp4");
    let part = dir.path().join("result.mp4.part");
    let checkpoint = dir.path().join("result.mp4.part.json");
    let (url, fixture, server) = start(bytes.clone(), mode).await;

    if matches!(mode, Mode::Hold) {
        let mut child = command(&url, &output, &hash(&bytes))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        time::timeout(Duration::from_secs(5), async {
            while std::fs::metadata(&part).map_or(0, |file| file.len()) < (bytes.len() / 3) as u64 {
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        child.kill().await.unwrap();
    } else {
        let first = download(&url, &output, &hash(&bytes)).await;
        assert!(
            !first.status.success(),
            "fixture should interrupt the first request"
        );
        assert!(
            String::from_utf8_lossy(&first.stderr).contains("reading response body"),
            "unexpected interrupted-download error: {:?}",
            first
        );
    }
    assert!(!output.exists());
    assert!(checkpoint.exists());
    let offset = std::fs::metadata(&part).unwrap().len();
    assert!(offset > 0 && offset < bytes.len() as u64);

    let mut expected = bytes.to_vec();
    if matches!(mode, Mode::Replace) {
        expected[0] ^= 1;
        *expected.last_mut().unwrap() ^= 1;
    }
    let expected_hash = hash(&expected);
    let second = download(&url, &output, &expected_hash).await;
    let seen = fixture.requests.lock().unwrap();
    let wanted_range = format!("bytes={offset}-");
    if matches!(mode, Mode::WeakTag) {
        assert_eq!(seen[1], (None, None));
    } else {
        assert_eq!(seen[1].0.as_deref(), Some(wanted_range.as_str()));
        assert_eq!(seen[1].1.as_deref(), Some(V1));
    }
    if matches!(mode, Mode::Unsatisfiable) {
        assert_eq!(seen[2], (None, None));
    }
    if matches!(mode, Mode::BadRange) {
        assert!(!second.status.success());
        assert!(String::from_utf8_lossy(&second.stderr).contains("does not match local offset"));
        assert_eq!(std::fs::metadata(part).unwrap().len(), offset);
        assert!(!output.exists());
    } else {
        assert!(
            second.status.success(),
            "{}",
            String::from_utf8_lossy(&second.stderr)
        );
        assert_eq!(std::fs::read(&output).unwrap(), expected);
        assert!(!part.exists());
        assert!(!checkpoint.exists());
        if let Some(path) = saved_video {
            std::fs::copy(&output, path).unwrap();
            println!(
                "Resumed {wanted_range}; total={}; SHA-256={expected_hash}",
                expected.len()
            );
        }
    }

    server.abort();
}

fn sample() -> Bytes {
    Bytes::from((0_u8..=250).cycle().take(512 * 1024).collect::<Vec<_>>())
}

#[tokio::test]
async fn resumes_after_disconnect() {
    exercise(Mode::Resume, sample(), None).await;
}

#[tokio::test]
async fn resumes_after_process_is_killed() {
    exercise(Mode::Hold, sample(), None).await;
}

#[tokio::test]
async fn replaces_an_object_of_the_same_size() {
    exercise(Mode::Replace, sample(), None).await;
}

#[tokio::test]
async fn restarts_if_server_ignores_range() {
    exercise(Mode::Ignore, sample(), None).await;
}

#[tokio::test]
async fn rejects_the_wrong_range_before_writing() {
    exercise(Mode::BadRange, sample(), None).await;
}

#[tokio::test]
async fn restarts_if_etag_is_weak() {
    exercise(Mode::WeakTag, sample(), None).await;
}

#[tokio::test]
async fn retries_with_full_get_after_416() {
    exercise(Mode::Unsatisfiable, sample(), None).await;
}

#[tokio::test]
async fn does_not_publish_on_hash_mismatch() {
    let bytes = sample();
    let dir = tempdir().unwrap();
    let output = dir.path().join("result.bin");
    let (url, _, server) = start(bytes, Mode::Resume).await;
    let wrong_hash = "00".repeat(32);
    assert!(!download(&url, &output, &wrong_hash).await.status.success());
    let second = download(&url, &output, &wrong_hash).await;
    assert!(!second.status.success());
    assert!(String::from_utf8_lossy(&second.stderr).contains("SHA-256 mismatch"));
    assert!(!output.exists());
    server.abort();
}

#[tokio::test]
#[ignore = "requires a local MP4 and an output path"]
async fn resumes_real_mp4() {
    let source = std::env::var_os("RESUME_VIDEO_SOURCE").expect("set RESUME_VIDEO_SOURCE");
    let output = std::env::var_os("RESUME_VIDEO_OUTPUT").expect("set RESUME_VIDEO_OUTPUT");
    let bytes = Bytes::from(std::fs::read(source).unwrap());
    exercise(Mode::Hold, bytes, Some(Path::new(&output))).await;
}
