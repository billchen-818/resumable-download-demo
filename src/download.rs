use std::{path::Path, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, Response, StatusCode, Url, header};
use sha2::{Digest, Sha256};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};

use crate::state::{Checkpoint, Paths};

pub(crate) async fn run(url: &Url, output: &Path, expected_hash: Option<[u8; 32]>) -> Result<u64> {
    ensure!(
        matches!(url.scheme(), "http" | "https"),
        "expected an HTTP(S) URL"
    );
    let paths = Paths::new(output);
    let _lock = paths.lock()?;
    ensure!(
        !output.try_exists()?,
        "output already exists: {}",
        output.display()
    );

    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(30))
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .build()?;

    let offset = paths.offset()?;
    let saved = paths.load()?;
    let resume = saved.as_ref().filter(|state| {
        offset > 0 && state.url == url.as_str() && state.total.is_none_or(|total| offset <= total)
    });
    let mut resume = resume.and_then(|state| {
        let etag = state.etag.as_deref().filter(|tag| is_strong_etag(tag))?;
        Some((state, etag))
    });

    let mut request = get(&client, url);
    if let Some((_, etag)) = resume {
        request = request
            .header(header::RANGE, format!("bytes={offset}-"))
            .header(header::IF_RANGE, etag);
    }
    let mut response = request.send().await.context("sending download request")?;
    if resume.is_some() && response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
        resume = None;
        response = get(&client, url)
            .send()
            .await
            .context("retrying from byte zero")?;
    }
    check_encoding(&response)?;

    let (mut file, written, expected_body) = match response.status() {
        StatusCode::OK => {
            let total = response.content_length();
            ensure!(
                total.is_some() || expected_hash.is_some(),
                "response length is unknown; provide --sha256 to verify completion"
            );
            let checkpoint = Checkpoint {
                url: url.to_string(),
                response_url: response.url().to_string(),
                etag: response
                    .headers()
                    .get(header::ETAG)
                    .and_then(|value| value.to_str().ok())
                    .filter(|tag| is_strong_etag(tag))
                    .map(str::to_owned),
                total,
            };
            let file = File::create(&paths.part)
                .await
                .context("resetting temporary file")?;
            paths
                .save(&checkpoint)
                .context("saving checkpoint before writing body")?;
            (file, 0, total)
        }
        StatusCode::PARTIAL_CONTENT => {
            let (saved, etag) = resume.context("unexpected partial response")?;
            let remaining = validate_tail(&response, saved, etag, offset)?;
            let file = OpenOptions::new()
                .append(true)
                .open(&paths.part)
                .await
                .context("opening temporary file for append")?;
            (file, offset, Some(remaining))
        }
        status => bail!("unexpected HTTP status: {status}"),
    };

    let transfer = write_body(&mut response, &mut file, expected_body).await;
    file.flush().await.context("flushing temporary file")?;
    let received = transfer?;
    let bytes = written
        .checked_add(received)
        .context("file size overflow")?;
    ensure!(
        file.metadata().await?.len() == bytes,
        "temporary file length changed"
    );
    if let Some(expected) = expected_hash {
        ensure!(
            hash_file(&paths.part).await? == expected,
            "SHA-256 mismatch"
        );
    }
    file.sync_all().await.context("syncing completed file")?;
    drop(file);

    // A hard link publishes the same inode atomically and cannot overwrite a destination.
    fs::hard_link(&paths.part, output)
        .await
        .context("publishing completed file")?;
    if let Err(error) = fs::remove_file(&paths.part).await {
        eprintln!("Downloaded file published; could not remove temporary file: {error}");
    } else if let Err(error) = paths.clear_checkpoint() {
        eprintln!("Downloaded file published; could not remove checkpoint: {error}");
    }
    Ok(bytes)
}

fn get(client: &Client, url: &Url) -> reqwest::RequestBuilder {
    client
        .get(url.clone())
        .header(header::ACCEPT_ENCODING, "identity")
}

fn check_encoding(response: &Response) -> Result<()> {
    for value in response.headers().get_all(header::CONTENT_ENCODING) {
        ensure!(
            value.to_str()?.trim().eq_ignore_ascii_case("identity"),
            "unsupported Content-Encoding"
        );
    }
    Ok(())
}

fn is_strong_etag(tag: &str) -> bool {
    tag.strip_prefix('"')
        .and_then(|tag| tag.strip_suffix('"'))
        .is_some_and(|tag| {
            tag.bytes()
                .all(|byte| byte == b'!' || (b'#'..=b'~').contains(&byte))
        })
}

fn validate_tail(response: &Response, saved: &Checkpoint, etag: &str, offset: u64) -> Result<u64> {
    ensure!(
        response.url().as_str() == saved.response_url,
        "resume response URL changed"
    );
    if let Some(response_etag) = response.headers().get(header::ETAG) {
        ensure!(response_etag.to_str()? == etag, "resume ETag mismatch");
    }
    let value = response
        .headers()
        .get(header::CONTENT_RANGE)
        .context("missing Content-Range")?
        .to_str()?;
    let (range, total) = value
        .strip_prefix("bytes ")
        .and_then(|value| value.split_once('/'))
        .context("invalid Content-Range")?;
    let (start, end) = range.split_once('-').context("invalid byte range")?;
    let (start, end, total) = (parse_byte(start)?, parse_byte(end)?, parse_byte(total)?);
    ensure!(
        start == offset,
        "Content-Range start {start} does not match local offset {offset}"
    );
    ensure!(
        start <= end && end.checked_add(1) == Some(total),
        "expected the complete remaining range"
    );
    ensure!(
        saved.total.is_none_or(|length| length == total),
        "remote length changed"
    );
    let remaining = total - start;
    ensure!(
        response
            .content_length()
            .is_none_or(|length| length == remaining),
        "response length disagrees with Content-Range"
    );
    Ok(remaining)
}

fn parse_byte(value: &str) -> Result<u64> {
    ensure!(
        !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()),
        "invalid byte count"
    );
    Ok(value.parse()?)
}

async fn write_body(
    response: &mut Response,
    file: &mut File,
    expected: Option<u64>,
) -> Result<u64> {
    let mut written = 0_u64;
    while let Some(chunk) = response.chunk().await.context("reading response body")? {
        let next = written
            .checked_add(u64::try_from(chunk.len())?)
            .context("response size overflow")?;
        ensure!(
            expected.is_none_or(|length| next <= length),
            "response exceeded expected length"
        );
        file.write_all(&chunk)
            .await
            .context("writing temporary file")?;
        written = next;
    }
    ensure!(
        expected.is_none_or(|length| length == written),
        "response ended before expected length"
    );
    Ok(written)
}

async fn hash_file(path: &Path) -> Result<[u8; 32]> {
    let mut file = File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let len = file.read(&mut buffer).await?;
        if len == 0 {
            return Ok(hasher.finalize().into());
        }
        hasher.update(&buffer[..len]);
    }
}
