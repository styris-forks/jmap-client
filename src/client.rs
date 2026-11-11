/*
 * Copyright Stalwart Labs LLC See the COPYING
 * file at the top-level directory of this distribution.
 *
 * Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
 * https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
 * <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
 * option. This file may not be copied, modified, or distributed
 * except according to those terms.
 */

use crate::{
    blob,
    core::{
        error::ProblemDetails,
        request::{self, Request},
        response,
        session::{Session, URLPart},
    },
    Error,
};
use ahash::AHashSet;
use encodify::base64;
use parking_lot::RwLock;
#[cfg(feature = "blocking")]
use reqwest::blocking::{Client as HttpClient, RequestBuilder, Response};
use reqwest::{
    header::{self},
    redirect, IntoUrl, Method, StatusCode,
};
#[cfg(feature = "async")]
use reqwest::{Client as HttpClient, RequestBuilder, Response};
use serde::de::DeserializeOwned;
use std::{
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

const DEFAULT_TIMEOUT_MS: u64 = 10 * 1000;
/// How long an idle pooled connection is kept. Servers commonly close idle connections after 60 seconds or
/// more, so dropping ours sooner avoids sending a request down a connection the server is closing.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// How much of an error response's body is kept in the error
const MAX_ERROR_BODY_CHARS: usize = 500;
static USER_AGENT: &str = concat!("jmap-client/", env!("CARGO_PKG_VERSION"));

type TrustedHosts = Arc<RwLock<AHashSet<String>>>;

#[derive(Debug, PartialEq, Eq)]
pub enum Credentials {
    Basic(String),
    Bearer(String),
}

pub struct Client {
    session: parking_lot::Mutex<Arc<Session>>,
    session_url: String,
    api_url: String,
    session_updated: AtomicBool,
    trusted_hosts: TrustedHosts,

    upload_url: Vec<URLPart<blob::URLParameter>>,
    download_url: Vec<URLPart<blob::URLParameter>>,
    #[cfg(feature = "async")]
    event_source_url: Vec<URLPart<crate::event_source::URLParameter>>,

    /// Pooled, and shared by every request this client makes. It holds no credentials, which are sent with
    /// each request instead, so one HTTP client can serve many JMAP clients signed in as different accounts.
    http: HttpClient,
    /// Sent with every request: the credentials, and where the request came from when that was set
    headers: header::HeaderMap,
    default_account_id: String,
    timeout: Duration,
    #[cfg(feature = "websockets")]
    pub(crate) accept_invalid_certs: bool,

    #[cfg(feature = "websockets")]
    pub(crate) authorization: String,
    #[cfg(feature = "websockets")]
    pub(crate) ws: tokio::sync::Mutex<Option<crate::client_ws::WsStream>>,
}

pub struct ClientBuilder {
    credentials: Option<Credentials>,
    trusted_hosts: AHashSet<String>,
    forwarded_for: Option<String>,
    accept_invalid_certs: bool,
    timeout: Duration,
    http: Option<HttpClient>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientBuilder {
    /// Creates a new `ClientBuilder`.
    ///
    /// Setting the credentials is required to connect to the JMAP API.
    pub fn new() -> Self {
        Self {
            credentials: None,
            trusted_hosts: AHashSet::new(),
            timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS),
            forwarded_for: None,
            accept_invalid_certs: false,
            http: None,
        }
    }

    /// Set up client credentials to connect to the JMAP API.
    ///
    /// The JMAP API URL is set using the [ClientBuilder.connect()](struct.ClientBuilder.html#method.connect) method.
    ///
    /// # Bearer authentication
    /// Pass a `&str` with the API Token.
    ///
    /// ```rust
    /// Client::new().credentials("some-api-token");
    /// ```
    ///
    /// Or use the longer form by using [Credentials::bearer()](enum.Credentials.html#method.bearer).
    /// ```rust
    /// let credentials = Credentials::bearer("some-api-token");
    /// Client::new().credentials(credentials);
    /// ```
    ///
    /// # Basic authentication
    /// Pass a `(&str, &str)` tuple, with the first position containing a username and the second containing a password.
    ///
    /// **It is not suggested to use this approach in production;** instead, if possible, use [Bearer authentication](struct.ClientBuilder.html#bearer-authentication).
    ///
    /// ```rust
    /// Client::new().credentials(("user@domain.com", "password"));
    /// ```
    ///
    /// Or use the longer form by using [Credentials::basic()](enum.Credentials.html#method.basic).
    /// ```rust
    /// let credentials = Credentials::basic("user@domain.com", "password");
    /// Client::new().credentials(credentials);
    /// ```
    pub fn credentials(mut self, credentials: impl Into<Credentials>) -> Self {
        self.credentials = Some(credentials.into());
        self
    }

    /// Set a timeout for all the requests to the JMAP API.
    ///
    /// The timeout can be changed after the `Client` has been created by using [Client.set_timeout()](struct.Client.html#method.set_timeout).
    ///
    /// By default the timeout is 10 seconds.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Accepts invalid certificates for all the requests to the JMAP API.
    ///
    /// By default certificates are validated.
    ///
    /// # Warning
    /// **It is not suggested to use this approach in production;** this method should be used only for testing and as a last resort.
    ///
    /// [Read more in the reqwest docs](https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html#method.danger_accept_invalid_certs)
    pub fn accept_invalid_certs(mut self, accept_invalid_certs: bool) -> Self {
        self.accept_invalid_certs = accept_invalid_certs;
        self
    }

    /// Set a list of trusted hosts that will be checked when a redirect is required.
    ///
    /// The list can be changed after the `Client` has been created by using [Client.set_follow_redirects()](struct.Client.html#method.set_follow_redirects).
    ///
    /// The client will follow at most 5 redirects.
    pub fn follow_redirects(
        mut self,
        trusted_hosts: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.trusted_hosts = trusted_hosts.into_iter().map(|h| h.into()).collect();
        self
    }

    /// Set the originating IP address of the client connecting to the JMAP API.
    pub fn forwarded_for(mut self, forwarded_for: IpAddr) -> Self {
        self.forwarded_for = Some(match forwarded_for {
            IpAddr::V4(addr) => format!("for={}", addr),
            IpAddr::V6(addr) => format!("for=\"{}\"", addr),
        });
        self
    }

    /// Makes requests through an HTTP client of the caller's, so several JMAP clients, even ones signed in as
    /// different accounts, share its connection pool. Credentials are sent with each request and never set on
    /// the HTTP client, so they stay apart.
    ///
    /// The HTTP client's own redirect policy, certificate checks and connect timeout apply, so this can't be
    /// combined with [follow_redirects](struct.ClientBuilder.html#method.follow_redirects) or
    /// [accept_invalid_certs](struct.ClientBuilder.html#method.accept_invalid_certs), and connecting fails
    /// if it is.
    pub fn http_client(mut self, http: HttpClient) -> Self {
        self.http = Some(http);
        self
    }

    /// Connects to the JMAP API Session URL.
    ///
    /// Setting up [Credentials](struct.ClientBuilder.html#method.credentials) must be done before calling this function.
    #[maybe_async::maybe_async]
    pub async fn connect(self, url: &str) -> crate::Result<Client> {
        let authorization = match self.credentials.expect("Missing credentials") {
            Credentials::Basic(s) => format!("Basic {}", s),
            Credentials::Bearer(s) => format!("Bearer {}", s),
        };
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::USER_AGENT,
            header::HeaderValue::from_static(USER_AGENT),
        );
        let mut credentials = header::HeaderValue::from_str(&authorization).map_err(|_| {
            Error::Internal("The credentials can't be sent in a header".to_string())
        })?;
        credentials.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, credentials);
        if let Some(forwarded_for) = self.forwarded_for {
            headers.insert(
                header::FORWARDED,
                header::HeaderValue::from_str(&forwarded_for).unwrap(),
            );
        }

        let trusted_hosts: TrustedHosts = Arc::new(RwLock::new(self.trusted_hosts));
        let http = match self.http {
            Some(_) if self.accept_invalid_certs || !trusted_hosts.read().is_empty() => {
                return Err(Error::Internal(
                    "A caller's HTTP client keeps its own redirect and certificate settings"
                        .to_string(),
                ));
            }
            Some(http) => http,
            None => HttpClient::builder()
                .connect_timeout(self.timeout)
                .pool_idle_timeout(POOL_IDLE_TIMEOUT)
                .danger_accept_invalid_certs(self.accept_invalid_certs)
                .redirect(redirect_policy(trusted_hosts.clone()))
                .build()?,
        };

        let session_url = format!("{}/.well-known/jmap", url);
        let session: Session = serde_json::from_slice(
            &Client::handle_error(
                http.get(&session_url)
                    .headers(headers.clone())
                    .timeout(self.timeout)
                    .send()
                    .await?,
            )
            .await?
            .bytes()
            .await?,
        )?;

        let default_account_id = session
            .primary_accounts()
            .next()
            .map(|a| a.1.to_string())
            .unwrap_or_default();

        Ok(Client {
            download_url: URLPart::parse(session.download_url())?,
            upload_url: URLPart::parse(session.upload_url())?,
            #[cfg(feature = "async")]
            event_source_url: URLPart::parse(session.event_source_url())?,
            api_url: session.api_url().to_string(),
            session: parking_lot::Mutex::new(Arc::new(session)),
            session_url,
            session_updated: true.into(),
            #[cfg(feature = "websockets")]
            accept_invalid_certs: self.accept_invalid_certs,
            trusted_hosts,
            http,
            #[cfg(feature = "websockets")]
            authorization,
            timeout: self.timeout,
            headers,
            default_account_id,
            #[cfg(feature = "websockets")]
            ws: None.into(),
        })
    }
}

/// Follows at most 5 redirects, and only to a trusted host. The hosts are read on each redirect, so changing
/// them later takes effect without rebuilding the HTTP client.
fn redirect_policy(trusted_hosts: TrustedHosts) -> redirect::Policy {
    redirect::Policy::custom(move |attempt| {
        if attempt.previous().len() > 5 {
            attempt.error("Too many redirects.")
        } else if matches!(attempt.url().host_str(), Some(host) if trusted_hosts.read().contains(host))
        {
            attempt.follow()
        } else {
            let message = format!(
                "Aborting redirect request to unknown host '{}'.",
                attempt.url().host_str().unwrap_or("")
            );
            attempt.error(message)
        }
    })
}

/// How long a 429 says to wait: its `Retry-After`, in seconds or as a date, or else the latest reset in its
/// `RateLimit` header, which lists each policy's state as `"requests";r=0;t=30`
fn retry_after(headers: &header::HeaderMap) -> Option<Duration> {
    let text = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());
    if let Some(value) = text("retry-after").map(str::trim) {
        if let Ok(seconds) = value.parse::<u64>() {
            return Some(Duration::from_secs(seconds));
        }
        if let Ok(date) = chrono::DateTime::parse_from_rfc2822(value) {
            let wait = date.with_timezone(&chrono::Utc) - chrono::Utc::now();
            return Some(wait.to_std().unwrap_or_default());
        }
    }
    text("ratelimit")?
        .split(',')
        .flat_map(|policy| policy.split(';').skip(1))
        .filter_map(|param| param.trim().strip_prefix("t=")?.parse::<u64>().ok())
        .max()
        .map(Duration::from_secs)
}

/// A short, single-line excerpt of an error response's body after its status
fn describe_failure(status: StatusCode, body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let mut excerpt = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if let Some((cut, _)) = excerpt.char_indices().nth(MAX_ERROR_BODY_CHARS) {
        excerpt.truncate(cut);
        excerpt.push('…');
    }
    if excerpt.is_empty() {
        status.to_string()
    } else {
        format!("{}: {}", status, excerpt)
    }
}

impl Client {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub fn set_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.timeout = timeout;
        self
    }

    /// Changes the hosts redirects are followed to, from the next request on. A client given its own HTTP client
    /// through [ClientBuilder.http_client()](struct.ClientBuilder.html#method.http_client) follows that
    /// client's redirect policy instead, so this changes nothing for it.
    pub fn set_follow_redirects(
        &mut self,
        trusted_hosts: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self {
        *self.trusted_hosts.write() = trusted_hosts.into_iter().map(|h| h.into()).collect();
        self
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn session(&self) -> Arc<Session> {
        self.session.lock().clone()
    }

    pub fn session_url(&self) -> &str {
        &self.session_url
    }

    pub fn headers(&self) -> &header::HeaderMap {
        &self.headers
    }

    /// A request through the shared HTTP client, carrying this client's credentials. With a timeout it has to
    /// finish within it, and without one only connecting is bounded, which suits long-lived streams.
    pub(crate) fn http_request(
        &self,
        method: Method,
        url: impl IntoUrl,
        timeout: Option<Duration>,
    ) -> RequestBuilder {
        let request = self.http.request(method, url).headers(self.headers.clone());
        match timeout {
            Some(timeout) => request.timeout(timeout),
            None => request,
        }
    }

    #[maybe_async::maybe_async]
    pub async fn send<R>(
        &self,
        request: &request::Request<'_>,
    ) -> crate::Result<response::Response<R>>
    where
        R: DeserializeOwned,
    {
        let response: response::Response<R> = serde_json::from_slice(
            &Client::handle_error(
                self.http_request(Method::POST, &self.api_url, Some(self.timeout))
                    .header(
                        header::CONTENT_TYPE,
                        header::HeaderValue::from_static("application/json"),
                    )
                    .body(serde_json::to_string(&request)?)
                    .send()
                    .await?,
            )
            .await?
            .bytes()
            .await?,
        )?;

        if response.session_state() != self.session.lock().state() {
            self.session_updated.store(false, Ordering::Relaxed);
        }

        Ok(response)
    }

    #[maybe_async::maybe_async]
    pub async fn refresh_session(&self) -> crate::Result<()> {
        let session: Session = serde_json::from_slice(
            &Client::handle_error(
                self.http_request(
                    Method::GET,
                    &self.session_url,
                    Some(Duration::from_millis(DEFAULT_TIMEOUT_MS)),
                )
                .send()
                .await?,
            )
            .await?
            .bytes()
            .await?,
        )?;
        *self.session.lock() = Arc::new(session);
        self.session_updated.store(true, Ordering::Relaxed);
        Ok(())
    }

    pub fn is_session_updated(&self) -> bool {
        self.session_updated.load(Ordering::Relaxed)
    }

    pub fn set_default_account_id(&mut self, defaul_account_id: impl Into<String>) -> &mut Self {
        self.default_account_id = defaul_account_id.into();
        self
    }

    pub fn default_account_id(&self) -> &str {
        &self.default_account_id
    }

    pub fn build(&self) -> Request<'_> {
        Request::new(self)
    }

    pub fn download_url(&self) -> &[URLPart<blob::URLParameter>] {
        &self.download_url
    }

    pub fn upload_url(&self) -> &[URLPart<blob::URLParameter>] {
        &self.upload_url
    }

    #[cfg(feature = "async")]
    pub fn event_source_url(&self) -> &[URLPart<crate::event_source::URLParameter>] {
        &self.event_source_url
    }

    /// Passes a successful response through, and turns any other into an error saying why. The body is read
    /// either way, which also frees a pooled connection for reuse. A 429 becomes
    /// [Error::RateLimited](enum.Error.html#variant.RateLimited) with how long its headers say to wait.
    #[maybe_async::maybe_async]
    pub async fn handle_error(response: Response) -> crate::Result<Response> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        if status == StatusCode::TOO_MANY_REQUESTS {
            let wait = retry_after(response.headers());
            let _ = response.bytes().await;
            return Err(Error::RateLimited(wait));
        }
        let is_problem = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/problem+json"));
        let body = response.bytes().await?;
        if is_problem {
            if let Ok(mut problem) = serde_json::from_slice::<ProblemDetails>(&body) {
                problem.status.get_or_insert(u32::from(status.as_u16()));
                return Err(Error::Problem(Box::new(problem)));
            }
        }
        Err(Error::Server(describe_failure(status, &body)))
    }
}

impl Credentials {
    pub fn basic(username: &str, password: &str) -> Self {
        Credentials::Basic(base64::STANDARD.encode(format!("{}:{}", username, password)))
    }

    pub fn bearer(token: impl Into<String>) -> Self {
        Credentials::Bearer(token.into())
    }
}

impl From<&str> for Credentials {
    fn from(s: &str) -> Self {
        Credentials::bearer(s.to_string())
    }
}

impl From<String> for Credentials {
    fn from(s: String) -> Self {
        Credentials::bearer(s)
    }
}

impl From<(&str, &str)> for Credentials {
    fn from((username, password): (&str, &str)) -> Self {
        Credentials::basic(username, password)
    }
}

impl From<(String, String)> for Credentials {
    fn from((username, password): (String, String)) -> Self {
        Credentials::basic(&username, &password)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::response::{Response, TaggedMethodResponse};

    #[test]
    fn test_deserialize() {
        let _r: Response<TaggedMethodResponse> = serde_json::from_slice(
            br#"{"sessionState": "123", "methodResponses": [[ "Email/query", {
                "accountId": "A1",
                "queryState": "abcdefg",
                "canCalculateChanges": true,
                "position": 0,
                "total": 101,
                "ids": [ "msg1023", "msg223", "msg110", "msg93", "msg91",
                    "msg38", "msg36", "msg33", "msg11", "msg1" ]
            }, "t0" ],
            [ "Email/get", {
                "accountId": "A1",
                "state": "123456",
                "list": [{
                    "id": "msg1023",
                    "threadId": "trd194"
                }, {
                    "id": "msg223",
                    "threadId": "trd114"
                }
                ],
                "notFound": []
            }, "t1" ],
            [ "Thread/get", {
                "accountId": "A1",
                "state": "123456",
                "list": [{
                    "id": "trd194",
                    "emailIds": [ "msg1020", "msg1021", "msg1023" ]
                }, {
                    "id": "trd114",
                    "emailIds": [ "msg201", "msg223" ]
                }
                ],
                "notFound": []
            }, "t2" ]]}"#,
        )
        .unwrap();
    }
}
