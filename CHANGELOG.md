Unreleased (fork)
================================
- Reuse one pooled HTTP client for every request instead of building one per request. Credentials are sent
  with each request, so `ClientBuilder::http_client` can share one HTTP client between clients signed in as
  different accounts.
- `set_follow_redirects` now takes effect on later requests.
- Errors from non-JMAP responses include the status and an excerpt of the body. Problem details are recognised
  when their content type has parameters, and fall back to the body excerpt when they can't be parsed.
- Method errors keep their `description`, and unknown method and set error types parse as `Other`.
- `Error` reports its underlying cause through `source()`.
- `Client::download_response` starts a blob download whose body can be streamed, without the API timeout.
- A 429 response is `Error::RateLimited`, with the wait its `Retry-After` or `RateLimit` header gives.

jmap-client 0.4.3
================================
- Replace `base64` with `encodify`.

jmap-client 0.4.2
================================
- JMAP compliance fixes. 

jmap-client 0.4.1
================================
- Use `rustls-tls-native-roots` for TLS support.

jmap-client 0.4.0
================================
- Calendar Alerts support.

jmap-client 0.3.3
================================
- JMAP for Sharing support.

jmap-client 0.3.2
================================
- Bump to `rustls` 0.22.

jmap-client 0.3.0
================================
- JMAP for Sieve Scripts DRAFT-14 support.
- Set timeouts using `Duration` instead of `u64`.
- SetError handling of unknown properties.
- Support deserializing non-IANA registered roles.

jmap-client 0.2.1
================================
- Using maybe_async to reduce duplicate code.
- Added `accept_invalid_certs` option to builder.

jmap-client 0.2.0
================================
- JMAP for Sieve Scripts support.

jmap-client 0.1.0
================================
- First release.
