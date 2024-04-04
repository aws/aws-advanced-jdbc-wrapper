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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

public class OktaCredentialsProviderFactory extends SamlCredentialsProviderFactory {

  private static final String TELEMETRY_FETCH_SAML = "Fetch OKTA SAML Assertion";

  private static final String OKTA_AWS_APP_NAME = "amazon_aws";
  private static final String SESSION_TOKEN = "sessionToken";
  private static final String ONE_TIME_TOKEN = "onetimetoken";
  private static final Logger LOGGER = Logger.getLogger(AdfsCredentialsProviderFactory.class.getName());
  private final PluginService pluginService;
  private final TelemetryFactory telemetryFactory;
  private final Supplier<CloseableHttpClient> httpClientSupplier;
  private TelemetryContext telemetryContext;

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public OktaCredentialsProviderFactory(final PluginService pluginService,
      final Supplier<CloseableHttpClient> httpClientSupplier) {
    this.pluginService = pluginService;
    this.telemetryFactory = this.pluginService.getTelemetryFactory();
    this.httpClientSupplier = httpClientSupplier;
  }

  @Override
  String getSamlAssertion(@NonNull Properties props) throws SQLException {
    this.telemetryContext = telemetryFactory.openTelemetryContext(TELEMETRY_FETCH_SAML, TelemetryTraceLevel.NESTED);

    try (final CloseableHttpClient httpClient = httpClientSupplier.get()) {
      final String sessionToken = getSessionToken(props);
      final String baseUri = getSamlUrl(props);
      final HttpUriRequest samlRequest = RequestBuilder
          .get()
          .setUri(baseUri)
          .addParameter(ONE_TIME_TOKEN, sessionToken)
          .build();

      try (final CloseableHttpResponse resp = httpClient.execute(samlRequest)) {
        final StatusLine statusLine = resp.getStatusLine();
        // Check HTTP Status Code is 2xx Success
        if (statusLine.getStatusCode() / 100 != 2) {
          throw new IOException(Messages.get("OktaCredentialsProviderFactory.samlRequestFailed",
              new Object[] {
                  statusLine.getStatusCode(),
                  statusLine.getReasonPhrase(),
                  EntityUtils.toString(resp.getEntity())}));
        }

        final HttpEntity responseEntity = resp.getEntity();
        final String responseHTMLAsString = EntityUtils.toString(responseEntity, "UTF-8");

        final Document document = Jsoup.parse(responseHTMLAsString);
        final Optional<String> samlResponseValue = Optional
            .ofNullable(document.selectFirst("[name=SAMLResponse]"))
            .map(field -> field.attr("value"));
        if (!samlResponseValue.isPresent()) {
          throw new SQLException(Messages.get("OktaCredentialsProviderFactory.invalidSamlResponse"));
        }

        return samlResponseValue.get();
      }

    } catch (final IOException e) {
      LOGGER.severe(Messages.get("SAMLCredentialsProviderFactory.getSamlAssertionFailed", new Object[] {e}));
      this.telemetryContext.setSuccess(false);
      this.telemetryContext.setException(e);
      throw new SQLException(e);
    } finally {
      this.telemetryContext.closeContext();
    }
  }

  /**
   * Fetches the sessionToken from Okta that will be used to fetch the SAML Assertion from AWS.
   *
   * @return Session token from Okta.
   * @throws SQLException When unable to parse the response body.
   */
  private String getSessionToken(final Properties props) throws SQLException {
    final String idpHost = OktaAuthPlugin.IDP_ENDPOINT.getString(props);
    final String idpUser = OktaAuthPlugin.IDP_USERNAME.getString(props);
    final String idpPassword = OktaAuthPlugin.IDP_PASSWORD.getString(props);

    final String sessionTokenEndpoint = "https://" + idpHost + "/api/v1/authn";

    try {
      final StringEntity requestBodyEntity = new StringEntity(
          "{\"username\":\"" + idpUser + "\",\"password\":\"" + idpPassword + "\"}", "UTF-8");

      final HttpUriRequest sessionTokenRequest = RequestBuilder
          .post()
          .setUri(sessionTokenEndpoint)
          .addHeader("Accept", "application/json")
          .addHeader("Content-Type", "application/json")
          .setEntity(requestBodyEntity)
          .build();

      try (final CloseableHttpClient httpClient = httpClientSupplier.get();
          CloseableHttpResponse response = httpClient.execute(sessionTokenRequest)) {
        final StatusLine statusLine = response.getStatusLine();
        if (statusLine.getStatusCode() / 100 != 2) {
          throw new SQLException(Messages.get("OktaCredentialsProviderFactory.sessionTokenRequestFailed"));
        }

        final HttpEntity responseEntity = response.getEntity();
        final String responseString = EntityUtils.toString(responseEntity, "UTF-8");
        final JsonNode jsonNode = OBJECT_MAPPER.readTree(responseString).get(SESSION_TOKEN);
        if (jsonNode == null) {
          throw new SQLException(Messages.get("OktaCredentialsProviderFactory.invalidSessionToken"));
        }
        return jsonNode.asText();
      }
    } catch (final IOException e) {
      throw new SQLException(Messages.get("OktaCredentialsProviderFactory.unableToOpenHttpClient"));
    }
  }

  private String getSamlUrl(final Properties props) throws IOException {
    final String idpHost = OktaAuthPlugin.IDP_ENDPOINT.getString(props);
    final String appId = OktaAuthPlugin.APP_ID.getString(props);
    final String baseUri = "https://" + idpHost + "/app/" + OKTA_AWS_APP_NAME + "/" + appId + "/sso/saml";
    SamlUtils.validateUrl(baseUri);
    return baseUri;
  }
}
