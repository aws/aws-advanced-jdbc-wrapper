/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.jdbc.plugin.federatedauth;

import java.security.GeneralSecurityException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;

/**
 * Provides a HttpClient so that requests to HTTP API can be made. This is used by the {@link
 * software.amazon.jdbc.plugin.federatedauth.AdfsCredentialsProviderFactory} to make HTTP calls to
 * ADFS HTTP endpoints that are not available via SDK.
 */
public class HttpClientFactory {
  private static final int MAX_REQUEST_RETRIES = 3;

  public CloseableHttpClient getCloseableHttpClient(
      final int socketTimeoutMs, final int connectionTimeoutMs, final boolean keySslInsecure)
      throws GeneralSecurityException {
    final RequestConfig rc =
        RequestConfig.custom()
            .setSocketTimeout(socketTimeoutMs)
            .setConnectTimeout(connectionTimeoutMs)
            .setExpectContinueEnabled(false)
            .setCookieSpec(CookieSpecs.STANDARD)
            .build();

    final HttpClientBuilder builder =
        HttpClients.custom()
            .setDefaultRequestConfig(rc)
            .setRedirectStrategy(new LaxRedirectStrategy())
            .setRetryHandler(new DefaultHttpRequestRetryHandler(MAX_REQUEST_RETRIES, true))
            .useSystemProperties(); // this is needed for proxy setting using system properties.

    if (keySslInsecure) {
      final SSLContext ctx = SSLContext.getInstance("TLSv1.2");
      final TrustManager[] tma =
          new TrustManager[] {new NonValidatingSSLSocketFactory.NonValidatingTrustManager()};
      ctx.init(null, tma, null);
      final SSLSocketFactory factory = ctx.getSocketFactory();

      final SSLConnectionSocketFactory sf =
          new SSLConnectionSocketFactory(factory, new NoopHostnameVerifier());

      builder.setSSLSocketFactory(sf);
    }

    return builder.build();
  }
}
