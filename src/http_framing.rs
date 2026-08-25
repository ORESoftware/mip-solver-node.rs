use tokio::io::AsyncReadExt;

#[derive(Debug)]
pub(crate) struct HttpEndpoint {
    pub(crate) addr: String,
    pub(crate) host_header: String,
    pub(crate) path_prefix: String,
}

pub(crate) fn parse_http_endpoint(base_url: &str) -> Result<HttpEndpoint, String> {
    let Some(rest) = base_url
        .trim()
        .trim_end_matches('/')
        .strip_prefix("http://")
    else {
        return Err("live-mutex URL must start with http://".to_string());
    };
    let (authority, path_prefix) = match rest.find('/') {
        Some(index) => (
            &rest[..index],
            rest[index..].trim_end_matches('/').to_string(),
        ),
        None => (rest, String::new()),
    };
    if authority.is_empty() {
        return Err("live-mutex URL is missing a host".to_string());
    }
    if authority.contains(['\r', '\n']) || path_prefix.contains(['\r', '\n']) {
        return Err("live-mutex URL must not contain CRLF characters".to_string());
    }
    let addr = if authority.starts_with('[') || authority.contains(':') {
        authority.to_string()
    } else {
        format!("{authority}:80")
    };
    Ok(HttpEndpoint {
        addr,
        host_header: authority.to_string(),
        path_prefix,
    })
}

pub(crate) fn http_path(prefix: &str, path: &str) -> String {
    if prefix.is_empty() {
        path.to_string()
    } else {
        format!("{prefix}{path}")
    }
}

pub(crate) fn decode_chunked_body(body: &[u8]) -> Result<Vec<u8>, String> {
    let mut decoded = Vec::new();
    let mut index = 0usize;
    loop {
        let Some(line_end) = body[index..]
            .windows(2)
            .position(|window| window == b"\r\n")
            .map(|position| index + position)
        else {
            return Err("chunked response missing chunk header terminator".to_string());
        };
        let size_text = std::str::from_utf8(&body[index..line_end])
            .map_err(|error| format!("invalid chunk header utf8: {error}"))?;
        let size_hex = size_text
            .split_once(';')
            .map(|(size, _)| size)
            .unwrap_or(size_text)
            .trim();
        let size = usize::from_str_radix(size_hex, 16)
            .map_err(|error| format!("invalid chunk size {size_text:?}: {error}"))?;
        index = line_end + 2;
        if size == 0 {
            return Ok(decoded);
        }
        let chunk_end = index.saturating_add(size);
        if chunk_end + 2 > body.len() {
            return Err("chunked response ended before declared chunk size".to_string());
        }
        if &body[chunk_end..chunk_end + 2] != b"\r\n" {
            return Err("chunked response missing chunk data terminator".to_string());
        }
        decoded.extend_from_slice(&body[index..chunk_end]);
        index = chunk_end + 2;
    }
}

fn header_value<'a>(headers: &'a str, name: &str) -> Option<&'a str> {
    headers.lines().skip(1).find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim()
            .eq_ignore_ascii_case(name)
            .then_some(value.trim())
    })
}

fn response_content_length(headers: &str) -> Result<Option<usize>, String> {
    header_value(headers, "content-length")
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|error| format!("invalid live-mutex content-length {value:?}: {error}"))
        })
        .transpose()
}

fn response_is_chunked(headers: &str) -> bool {
    header_value(headers, "transfer-encoding").is_some_and(|value| {
        value
            .split(',')
            .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
    })
}

fn chunked_body_complete(body: &[u8]) -> bool {
    let mut index = 0usize;
    loop {
        let Some(line_end) = body[index..]
            .windows(2)
            .position(|window| window == b"\r\n")
            .map(|position| index + position)
        else {
            return false;
        };
        let Ok(size_text) = std::str::from_utf8(&body[index..line_end]) else {
            return false;
        };
        let size_hex = size_text
            .split_once(';')
            .map(|(size, _)| size)
            .unwrap_or(size_text)
            .trim();
        let Ok(size) = usize::from_str_radix(size_hex, 16) else {
            return false;
        };
        index = line_end + 2;
        if size == 0 {
            return body[index..].windows(2).any(|window| window == b"\r\n");
        }
        let chunk_end = index.saturating_add(size);
        if chunk_end + 2 > body.len() {
            return false;
        }
        if &body[chunk_end..chunk_end + 2] != b"\r\n" {
            return false;
        }
        index = chunk_end + 2;
    }
}

pub(crate) async fn read_http_response<R>(
    stream: &mut R,
    max_response_bytes: u64,
) -> Result<Vec<u8>, String>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut response = Vec::new();
    let mut header_end: Option<usize> = None;
    let mut expected_len: Option<usize> = None;
    let mut chunked = false;
    let mut buf = [0u8; 4096];
    loop {
        if response.len() as u64 > max_response_bytes {
            return Err(format!(
                "live-mutex response exceeded {} bytes",
                max_response_bytes
            ));
        }
        if let Some(end) = header_end {
            let body_start = end + 4;
            let body_len = response.len().saturating_sub(body_start);
            if let Some(length) = expected_len {
                if body_len >= length {
                    response.truncate(body_start + length);
                    break;
                }
            } else if chunked && chunked_body_complete(&response[body_start..]) {
                break;
            }
        }
        let read = stream
            .read(&mut buf)
            .await
            .map_err(|error| format!("read response: {error}"))?;
        if read == 0 {
            break;
        }
        response.extend_from_slice(&buf[..read]);
        if header_end.is_none() {
            if let Some(end) = response.windows(4).position(|window| window == b"\r\n\r\n") {
                let headers = std::str::from_utf8(&response[..end]).map_err(|error| {
                    format!("live-mutex response headers are not utf8: {error}")
                })?;
                expected_len = response_content_length(headers)?;
                chunked = response_is_chunked(headers);
                header_end = Some(end);
            }
        }
    }
    Ok(response)
}
