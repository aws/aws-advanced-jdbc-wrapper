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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
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

class OktaCredentialsProviderFactoryTest {

  private static final String USERNAME = "someFederatedUsername@example.com";
  private static final String PASSWORD = "ec2amazab3cdef";
  private static final String ENDPOINT = "example.okta.com";
  private static final String APPLICATION_ID = "example.okta.com";
  @Mock private PluginService mockPluginService;
  @Mock private TelemetryFactory mockTelemetryFactory;
  @Mock private TelemetryContext mockTelemetryContext;
  @Mock private Supplier<CloseableHttpClient> mockHttpClientSupplier;
  @Mock private CloseableHttpClient mockHttpClient;
  @Mock private CloseableHttpResponse mockResponse;
  @Mock private HttpEntity mockEntity;
  @Mock private StatusLine mockStatusLine;
  private OktaCredentialsProviderFactory oktaCredentialsProviderFactory;
  private Properties props;
  private AutoCloseable closable;

  @BeforeEach
  void setUp() throws IOException {
    closable = MockitoAnnotations.openMocks(this);

    this.props = new Properties();
    this.props.setProperty(OktaAuthPlugin.IDP_ENDPOINT.name, ENDPOINT);
    this.props.setProperty(OktaAuthPlugin.APP_ID.name, APPLICATION_ID);
    this.props.setProperty(OktaAuthPlugin.IDP_USERNAME.name, USERNAME);
    this.props.setProperty(OktaAuthPlugin.IDP_PASSWORD.name, PASSWORD);

    when(mockPluginService.getTelemetryFactory()).thenReturn(mockTelemetryFactory);
    when(mockTelemetryFactory.openTelemetryContext(any(), any())).thenReturn(mockTelemetryContext);
    when(mockHttpClientSupplier.get()).thenReturn(mockHttpClient);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(200);
    when(mockHttpClient.execute(any())).thenReturn(mockResponse);

    this.oktaCredentialsProviderFactory = new OktaCredentialsProviderFactory(mockPluginService, mockHttpClientSupplier);
  }

  @AfterEach
  void tearDown() throws Exception {
    closable.close();
  }

  @Test
  void testGetSamlAssertion() throws IOException, SQLException, URISyntaxException {
    final String sessionTokenResponse = getResource("okta/session.txt");
    final String samlAssertionResponse = getResource("okta/saml-assertion.html");
    final String expectedSessionToken = getResource("okta/expected-session-token.txt");
    final String expectedAssertion = getResource("okta/assertion.txt");
    final URI expectedUri = new URI(
        "https://example.okta.com/app/amazon_aws/example.okta.com/sso/saml?onetimetoken=" + expectedSessionToken);
    final HttpEntity sessionTokenEntity = new StringEntity(sessionTokenResponse);
    final HttpEntity samlAssertionEntity = new StringEntity(samlAssertionResponse);
    final String expectedSessionTokenEndpoint = "https://" + ENDPOINT + "/api/v1/authn";
    final String expectedSessionTokenRequestEntity =
        "{\"username\":\"" + USERNAME + "\",\"password\":\"" + PASSWORD + "\"}";

    when(mockResponse.getEntity()).thenReturn(sessionTokenEntity, samlAssertionEntity);

    final String samlAssertion = this.oktaCredentialsProviderFactory.getSamlAssertion(props);
    assertEquals(expectedAssertion, samlAssertion);

    final ArgumentCaptor<HttpUriRequest> httpPostArgumentCaptor = ArgumentCaptor.forClass(HttpUriRequest.class);
    verify(mockHttpClient, times(2)).execute(httpPostArgumentCaptor.capture());
    final List<HttpUriRequest> actualCaptures = httpPostArgumentCaptor.getAllValues();
    final HttpEntityEnclosingRequest sessionTokenRequest = (HttpEntityEnclosingRequest) actualCaptures.get(0);
    final String content = EntityUtils.toString(sessionTokenRequest.getEntity());
    final HttpUriRequest samlRequest = actualCaptures.get(1);
    assertEquals(expectedSessionTokenEndpoint, sessionTokenRequest.getRequestLine().getUri());
    assertEquals(expectedSessionTokenRequestEntity, content);
    assertEquals(expectedUri, samlRequest.getURI());
  }

  private String getResource(final String fileName) throws IOException {
    return IOUtils.toString(
        Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream(fileName)),
        "UTF-8");
  }
}