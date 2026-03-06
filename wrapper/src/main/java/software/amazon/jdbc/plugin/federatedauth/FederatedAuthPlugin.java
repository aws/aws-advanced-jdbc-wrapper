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

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.TokenInfo;
import software.amazon.jdbc.plugin.iam.IamTokenUtility;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.IamAuthUtils;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

public class FederatedAuthPlugin extends BaseSamlAuthPlugin {
  private static final int DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60 - 30;
  private static final int DEFAULT_HTTP_TIMEOUT_MILLIS = 60000;
  public static final AwsWrapperProperty IDP_ENDPOINT = new AwsWrapperProperty("idpEndpoint", null,
      "The hosting URL of the Identity Provider");
  public static final AwsWrapperProperty IDP_PORT =
      new AwsWrapperProperty("idpPort", "443", "The hosting port of Identity Provider");
  public static final AwsWrapperProperty RELAYING_PARTY_ID =
      new AwsWrapperProperty("rpIdentifier", "urn:amazon:webservices", "The relaying party identifier");
  public static final AwsWrapperProperty IAM_ROLE_ARN =
      new AwsWrapperProperty("iamRoleArn", null, "The ARN of the IAM Role that is to be assumed.");
  public static final AwsWrapperProperty IAM_IDP_ARN =
      new AwsWrapperProperty("iamIdpArn", null, "The ARN of the Identity Provider");
  public static final AwsWrapperProperty IAM_REGION = new AwsWrapperProperty("iamRegion", null,
      "Overrides AWS region that is used to generate the IAM token");
  public static final AwsWrapperProperty IAM_TOKEN_EXPIRATION = new AwsWrapperProperty("iamTokenExpiration",
      String.valueOf(DEFAULT_TOKEN_EXPIRATION_SEC), "IAM token cache expiration in seconds");
  public static final AwsWrapperProperty IDP_USERNAME =
      new AwsWrapperProperty("idpUsername", null, "The federated user name");
  public static final AwsWrapperProperty IDP_PASSWORD = new AwsWrapperProperty("idpPassword", null,
      "The federated user password");
  public static final AwsWrapperProperty IAM_HOST = new AwsWrapperProperty(
      "iamHost", null,
      "Overrides the host that is used to generate the IAM token");
  public static final AwsWrapperProperty IAM_DEFAULT_PORT = new AwsWrapperProperty("iamDefaultPort", "-1",
      "Overrides default port that is used to generate the IAM token");
  public static final AwsWrapperProperty HTTP_CLIENT_SOCKET_TIMEOUT = new AwsWrapperProperty(
      "httpClientSocketTimeout", String.valueOf(DEFAULT_HTTP_TIMEOUT_MILLIS),
      "The socket timeout value in milliseconds for the HttpClient used by the FederatedAuthPlugin");
  public static final AwsWrapperProperty HTTP_CLIENT_CONNECT_TIMEOUT = new AwsWrapperProperty(
      "httpClientConnectTimeout", String.valueOf(DEFAULT_HTTP_TIMEOUT_MILLIS),
      "The connect timeout value in milliseconds for the HttpClient used by the FederatedAuthPlugin");
  public static final AwsWrapperProperty SSL_INSECURE = new AwsWrapperProperty("sslInsecure", "false",
      "Whether or not the SSL session is to be secure and the sever's certificates will be verified");
  public static AwsWrapperProperty
      IDP_NAME = new AwsWrapperProperty("idpName", "adfs", "The name of the Identity Provider implementation used");
  public static final AwsWrapperProperty DB_USER =
      new AwsWrapperProperty("dbUser", null, "The database user used to access the database");
  protected static final Pattern SAML_RESPONSE_PATTERN = Pattern.compile("SAMLResponse\\W+value=\"(?<saml>[^\"]+)\"");
  protected static final String SAML_RESPONSE_PATTERN_GROUP = "saml";

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add(JdbcMethod.CONNECT.methodName);
          add(JdbcMethod.FORCECONNECT.methodName);
        }
      });

  static {
    PropertyDefinition.registerPluginProperties(FederatedAuthPlugin.class);
  }

  private final TelemetryGauge cacheSizeGauge;
  private final TelemetryCounter fetchTokenCounter;

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  public FederatedAuthPlugin(final FullServicesContainer servicesContainer,
      final CredentialsProviderFactory credentialsProviderFactory) {
    this(servicesContainer, credentialsProviderFactory, new RdsUtils(), IamAuthUtils.getTokenUtility());
  }

  FederatedAuthPlugin(
      final FullServicesContainer servicesContainer,
      final CredentialsProviderFactory credentialsProviderFactory,
      final RdsUtils rdsUtils,
      final IamTokenUtility tokenUtils) {
    super(servicesContainer, credentialsProviderFactory, rdsUtils, tokenUtils);
    try {
      Class.forName("software.amazon.awssdk.services.sts.model.AssumeRoleWithSamlRequest");
    } catch (final ClassNotFoundException e) {
      try {
        Class.forName("shaded.software.amazon.awssdk.services.sts.model.AssumeRoleWithSamlRequest");
      } catch (final ClassNotFoundException e2) {
        throw new RuntimeException(Messages.get("SamlAuthPlugin.javaStsSdkNotInClasspath"));
      }
    }

    this.cacheSizeGauge = this.telemetryFactory.createGauge("federatedAuth.tokenCache.size",
        () -> (long) this.servicesContainer.getStorageService().size(TokenInfo.class));
    this.fetchTokenCounter = this.telemetryFactory.createCounter("federatedAuth.fetchToken.count");
  }


  @Override
  String getDbUserProperty(Properties props) {
    return DB_USER.getString(props);
  }

  @Override
  String getIamHostProperty(Properties props) {
    return IAM_HOST.getString(props);
  }

  @Override
  int getIamPortProperty(Properties props) {
    try {
      return IAM_DEFAULT_PORT.getInteger(props);
    } catch (NumberFormatException e) {
      // Return 0 if IAM_DEFAULT_PORT returns null or an empty string.
      return 0;
    }
  }

  @Override
  int getIamTokenExpiration(Properties props) {
    try {
      return IAM_TOKEN_EXPIRATION.getInteger(props);
    } catch (NumberFormatException e) {
      // Return 0 if IAM_DEFAULT_PORT returns null or an empty string.
      return DEFAULT_TOKEN_EXPIRATION_SEC;
    }
  }

  @Override
  void checkIdpCredentialsWithFallback(Properties props) {
    this.samlUtils.checkIdpCredentialsWithFallback(IDP_USERNAME, IDP_PASSWORD, props);
  }
}
