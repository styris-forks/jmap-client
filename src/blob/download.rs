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

use reqwest::Method;

use crate::{client::Client, core::session::URLPart};

impl Client {
    fn blob_download_url(&self, blob_id: &str) -> String {
        let account_id = self.default_account_id();
        let mut download_url = String::with_capacity(
            self.session().download_url().len() + account_id.len() + blob_id.len(),
        );

        for part in self.download_url() {
            match part {
                URLPart::Value(value) => {
                    download_url.push_str(value);
                }
                URLPart::Parameter(param) => match param {
                    super::URLParameter::AccountId => {
                        download_url.push_str(account_id);
                    }
                    super::URLParameter::BlobId => {
                        download_url.push_str(blob_id);
                    }
                    super::URLParameter::Name => {
                        download_url.push_str("none");
                    }
                    super::URLParameter::Type => {
                        download_url.push_str("application/octet-stream");
                    }
                },
            }
        }
        download_url
    }

    #[maybe_async::maybe_async]
    pub async fn download(&self, blob_id: &str) -> crate::Result<Vec<u8>> {
        Client::handle_error(
            self.http_request(
                Method::GET,
                self.blob_download_url(blob_id),
                Some(self.timeout()),
            )
            .send()
            .await?,
        )
        .await?
        .bytes()
        .await
        .map(|bytes| bytes.to_vec())
        .map_err(|err| err.into())
    }

    /// Starts downloading a blob, for its body to be streamed as it comes. Only the HTTP client's own timeouts
    /// apply, since a big blob can take longer than any API call should.
    #[cfg(not(feature = "blocking"))]
    pub async fn download_response(&self, blob_id: &str) -> crate::Result<reqwest::Response> {
        Client::handle_error(
            self.http_request(Method::GET, self.blob_download_url(blob_id), None)
                .send()
                .await?,
        )
        .await
    }
}
