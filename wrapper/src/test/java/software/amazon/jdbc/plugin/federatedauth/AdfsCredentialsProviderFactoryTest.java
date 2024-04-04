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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;

class AdfsCredentialsProviderFactoryTest {

  private static final String USERNAME = "someFederatedUsername@example.com";
  private static final String PASSWORD = "somePassword";
  @Mock private PluginService mockPluginService;
  @Mock private TelemetryFactory mockTelemetryFactory;
  @Mock private TelemetryContext mockTelemetryContext;
  @Mock private Supplier<CloseableHttpClient> mockHttpClientSupplier;
  @Mock private CloseableHttpClient mockHttpClient;
  @Mock private CloseableHttpResponse mockHttpGetSignInPageResponse;
  @Mock private CloseableHttpResponse mockHttpPostSignInResponse;
  @Mock private StatusLine mockStatusLine;
  @Mock private HttpEntity mockSignInPageHttpEntity;
  @Mock private HttpEntity mockSamlHttpEntity;
  private AdfsCredentialsProviderFactory adfsCredentialsProviderFactory;
  private Properties props;
  private AutoCloseable closeable;

  @BeforeEach
  public void init() throws IOException {
    closeable = MockitoAnnotations.openMocks(this);

    this.props = new Properties();
    this.props.setProperty(FederatedAuthPlugin.IDP_ENDPOINT.name, "ec2amaz-ab3cdef.example.com");
    this.props.setProperty(FederatedAuthPlugin.IDP_USERNAME.name, USERNAME);
    this.props.setProperty(FederatedAuthPlugin.IDP_PASSWORD.name, PASSWORD);

    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(any(), any())).thenReturn(mockTelemetryContext);
    when(mockHttpClientSupplier.get()).thenReturn(mockHttpClient);
    when(mockHttpGetSignInPageResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(200);
    when(mockHttpGetSignInPageResponse.getEntity()).thenReturn(mockSignInPageHttpEntity);

    final String signinPageHtml = IOUtils.toString(
        this.getClass().getClassLoader().getResourceAsStream("federated_auth/adfs-sign-in-page.html"), "UTF-8");
    final InputStream signInPageHtmlInputStream = new ByteArrayInputStream(signinPageHtml.getBytes());
    when(mockSignInPageHttpEntity.getContent()).thenReturn(signInPageHtmlInputStream);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockHttpPostSignInResponse);
    when(mockHttpPostSignInResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockHttpPostSignInResponse.getEntity()).thenReturn(mockSamlHttpEntity);

    final String adfsSamlHtml = IOUtils.toString(
        this.getClass().getClassLoader().getResourceAsStream("federated_auth/adfs-saml.html"), "UTF-8");
    final InputStream samlHtmlInputStream = new ByteArrayInputStream(adfsSamlHtml.getBytes());
    when(mockSamlHttpEntity.getContent()).thenReturn(samlHtmlInputStream);

    this.adfsCredentialsProviderFactory = new AdfsCredentialsProviderFactory(mockPluginService, mockHttpClientSupplier);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void test() throws IOException, SQLException {
    when(mockHttpClient.execute(any(HttpUriRequest.class))).thenReturn(
        mockHttpGetSignInPageResponse,
        mockHttpPostSignInResponse);
    final String correctSamlAssertion = IOUtils.toString(
            Objects.requireNonNull(
                this.getClass().getClassLoader().getResourceAsStream("federated_auth/saml-assertion.txt")),
            "UTF-8")
        .replace("\n", "")
        .replace("\r", "");
    final String samlAssertion = this.adfsCredentialsProviderFactory.getSamlAssertion(props);
    assertEquals(correctSamlAssertion, samlAssertion);

    final ArgumentCaptor<HttpUriRequest> httpPostArgumentCaptor = ArgumentCaptor.forClass(HttpUriRequest.class);
    verify(mockHttpClient, times(2)).execute(httpPostArgumentCaptor.capture());
    final HttpEntityEnclosingRequest actualHttpPost = (HttpEntityEnclosingRequest) httpPostArgumentCaptor.getValue();
    final String content = EntityUtils.toString(actualHttpPost.getEntity());
    final String[] params = content.split("&");
    assertEquals("UserName=" + USERNAME.replace("@", "%40"), params[0]);
    assertEquals("Password=" + PASSWORD, params[1]);
    assertEquals("Kmsi=true", params[2]);
    assertEquals("AuthMethod=FormsAuthentication", params[3]);
  }
}
