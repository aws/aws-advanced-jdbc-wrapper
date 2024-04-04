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

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

public class AdfsCredentialsProviderFactory extends SamlCredentialsProviderFactory {

  public static final String IDP_NAME = "adfs";
  private static final String TELEMETRY_FETCH_SAML = "Fetch ADFS SAML Assertion";
  private static final Pattern INPUT_TAG_PATTERN = Pattern.compile("<input(.+?)/>", Pattern.DOTALL);
  private static final Pattern FORM_ACTION_PATTERN = Pattern.compile("<form.*?action=\"([^\"]+)\"");
  private static final Logger LOGGER = Logger.getLogger(AdfsCredentialsProviderFactory.class.getName());
  private final PluginService pluginService;
  private final TelemetryFactory telemetryFactory;
  private final Supplier<CloseableHttpClient> httpClientSupplier;
  private TelemetryContext telemetryContext;

  public AdfsCredentialsProviderFactory(final PluginService pluginService,
      final Supplier<CloseableHttpClient> httpClientSupplier) {
    this.pluginService = pluginService;
    this.telemetryFactory = this.pluginService.getTelemetryFactory();
    this.httpClientSupplier = httpClientSupplier;
  }

  @Override
  String getSamlAssertion(final @NonNull Properties props) throws SQLException {
    this.telemetryContext = telemetryFactory.openTelemetryContext(TELEMETRY_FETCH_SAML, TelemetryTraceLevel.NESTED);

    try (final CloseableHttpClient httpClient = httpClientSupplier.get()) {
      String uri = getSignInPageUrl(props);
      final String signInPageBody = getSignInPageBody(httpClient, uri);
      final String action = getFormActionFromHtmlBody(signInPageBody);

      if (!StringUtils.isNullOrEmpty(action) && action.startsWith("/")) {
        uri = getFormActionUrl(props, action);
      }

      final List<NameValuePair> params = getParametersFromHtmlBody(signInPageBody, props);
      final String content = getFormActionBody(httpClient, uri, params);

      final Matcher matcher = FederatedAuthPlugin.SAML_RESPONSE_PATTERN.matcher(content);
      if (!matcher.find()) {
        throw new IOException(Messages.get("AdfsCredentialsProviderFactory.failedLogin", new Object[] {content}));
      }

      // return SAML Response value
      return matcher.group(FederatedAuthPlugin.SAML_RESPONSE_PATTERN_GROUP);
    } catch (final IOException e) {
      LOGGER.severe(Messages.get("SAMLCredentialsProviderFactory.getSamlAssertionFailed", new Object[] {e}));
      this.telemetryContext.setSuccess(false);
      this.telemetryContext.setException(e);
      throw new SQLException(e);
    } finally {
      this.telemetryContext.closeContext();
    }
  }

  private String getSignInPageBody(final CloseableHttpClient httpClient, final String uri) throws IOException {
    LOGGER.finest(Messages.get("AdfsCredentialsProviderFactory.signOnPageUrl", new Object[] {uri}));
    SamlUtils.validateUrl(uri);
    try (final CloseableHttpResponse resp = httpClient.execute(new HttpGet(uri))) {
      final StatusLine statusLine = resp.getStatusLine();
      // Check HTTP Status Code is 2xx Success
      if (statusLine.getStatusCode() / 100 != 2) {
        throw new IOException(Messages.get("AdfsCredentialsProviderFactory.signOnPageRequestFailed",
            new Object[] {
                statusLine.getStatusCode(),
                statusLine.getReasonPhrase(),
                EntityUtils.toString(resp.getEntity())}));
      }
      return EntityUtils.toString(resp.getEntity());
    }
  }

  private String getFormActionBody(final CloseableHttpClient httpClient, final String uri,
      final List<NameValuePair> params) throws IOException {
    LOGGER.finest(Messages.get("AdfsCredentialsProviderFactory.signOnPagePostActionUrl", new Object[] {uri}));
    SamlUtils.validateUrl(uri);

    final HttpUriRequest request = RequestBuilder
        .post()
        .setUri(uri)
        .setEntity(new UrlEncodedFormEntity(params))
        .build();
    try (final CloseableHttpResponse resp = httpClient.execute(request)) {
      final StatusLine statusLine = resp.getStatusLine();
      // Check HTTP Status Code is 2xx Success
      if (statusLine.getStatusCode() / 100 != 2) {
        throw new IOException(Messages.get("AdfsCredentialsProviderFactory.signOnPagePostActionRequestFailed",
            new Object[] {
                statusLine.getStatusCode(),
                statusLine.getReasonPhrase(),
                EntityUtils.toString(resp.getEntity())}));
      }
      return EntityUtils.toString(resp.getEntity());
    }
  }

  private String getSignInPageUrl(final Properties props) {
    return "https://" + FederatedAuthPlugin.IDP_ENDPOINT.getString(props) + ':'
        + FederatedAuthPlugin.IDP_PORT.getString(props) + "/adfs/ls/IdpInitiatedSignOn.aspx?loginToRp="
        + FederatedAuthPlugin.RELAYING_PARTY_ID.getString(props);
  }

  private String getFormActionUrl(final Properties props, final String action) {
    return "https://" + FederatedAuthPlugin.IDP_ENDPOINT.getString(props) + ':'
        + FederatedAuthPlugin.IDP_PORT.getString(props) + action;
  }

  private List<String> getInputTagsFromHTML(final String body) {
    final Set<String> distinctInputTags = new HashSet<>();
    final List<String> inputTags = new ArrayList<>();
    final Matcher inputTagMatcher = INPUT_TAG_PATTERN.matcher(body);
    while (inputTagMatcher.find()) {
      final String tag = inputTagMatcher.group(0);
      final String tagNameLower = getValueByKey(tag, "name").toLowerCase();
      if (!tagNameLower.isEmpty() && distinctInputTags.add(tagNameLower)) {
        inputTags.add(tag);
      }
    }
    return inputTags;
  }

  private String getValueByKey(final String input, final String key) {
    final Pattern keyValuePattern = Pattern.compile("(" + Pattern.quote(key) + ")\\s*=\\s*\"(.*?)\"");
    final Matcher keyValueMatcher = keyValuePattern.matcher(input);
    if (keyValueMatcher.find()) {
      return escapeHtmlEntity(keyValueMatcher.group(2));
    }
    return "";
  }

  private String escapeHtmlEntity(final String html) {
    final StringBuilder sb = new StringBuilder(html.length());
    int i = 0;
    final int length = html.length();
    while (i < length) {
      final char c = html.charAt(i);
      if (c != '&') {
        sb.append(c);
        i++;
        continue;
      }

      if (html.startsWith("&amp;", i)) {
        sb.append('&');
        i += 5;
      } else if (html.startsWith("&apos;", i)) {
        sb.append('\'');
        i += 6;
      } else if (html.startsWith("&quot;", i)) {
        sb.append('"');
        i += 6;
      } else if (html.startsWith("&lt;", i)) {
        sb.append('<');
        i += 4;
      } else if (html.startsWith("&gt;", i)) {
        sb.append('>');
        i += 4;
      } else {
        sb.append(c);
        ++i;
      }
    }
    return sb.toString();
  }

  private List<NameValuePair> getParametersFromHtmlBody(final String body, final @NonNull Properties props) {
    final List<NameValuePair> parameters = new ArrayList<>();
    for (final String inputTag : getInputTagsFromHTML(body)) {
      final String name = getValueByKey(inputTag, "name");
      final String value = getValueByKey(inputTag, "value");
      final String nameLower = name.toLowerCase();

      if (nameLower.contains("username")) {
        parameters.add(new BasicNameValuePair(name, FederatedAuthPlugin.IDP_USERNAME.getString(props)));
      } else if (nameLower.contains("authmethod")) {
        if (!value.isEmpty()) {
          parameters.add(new BasicNameValuePair(name, value));
        }
      } else if (nameLower.contains("password")) {
        parameters
            .add(new BasicNameValuePair(name, FederatedAuthPlugin.IDP_PASSWORD.getString(props)));
      } else if (!name.isEmpty()) {
        parameters.add(new BasicNameValuePair(name, value));
      }
    }
    return parameters;
  }

  private String getFormActionFromHtmlBody(final String body) {
    final Matcher m = FORM_ACTION_PATTERN.matcher(body);
    if (m.find()) {
      return escapeHtmlEntity(m.group(1));
    }
    return null;
  }
}
