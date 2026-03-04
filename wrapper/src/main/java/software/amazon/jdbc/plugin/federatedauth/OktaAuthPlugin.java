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

import java.util.Properties;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.iam.IamTokenUtility;
import software.amazon.jdbc.util.IamAuthUtils;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUtils;

public class OktaAuthPlugin extends BaseSamlAuthPlugin {

  private static final int DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60 - 30;
  private static final int DEFAULT_HTTP_TIMEOUT_MILLIS = 60000;

  public static final AwsWrapperProperty IDP_ENDPOINT = new AwsWrapperProperty("idpEndpoint", null,
      "The hosting URL of the Identity Provider");
  public static final AwsWrapperProperty APP_ID = new AwsWrapperProperty("appId", null,
      "The ID of the AWS application configured on Okta");
  public static final AwsWrapperProperty IAM_ROLE_ARN =
      new AwsWrapperProperty("iamRoleArn", null, "The ARN of the IAM Role that is to be assumed.");
  public static final AwsWrapperProperty IAM_IDP_ARN =
      new AwsWrapperProperty("iamIdpArn", null, "The ARN of the Identity Provider");
  public static final AwsWrapperProperty IAM_REGION = new AwsWrapperProperty(BaseSamlAuthPlugin.IAM_REGION_NAME, null,
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
      "Overrides default port that is used to generate the authentication token");
  public static final AwsWrapperProperty HTTP_CLIENT_SOCKET_TIMEOUT = new AwsWrapperProperty(
      "httpClientSocketTimeout", String.valueOf(DEFAULT_HTTP_TIMEOUT_MILLIS),
      "The socket timeout value in milliseconds for the HttpClient used by the OktaAuthPlugin");
  public static final AwsWrapperProperty HTTP_CLIENT_CONNECT_TIMEOUT = new AwsWrapperProperty(
      "httpClientConnectTimeout", String.valueOf(DEFAULT_HTTP_TIMEOUT_MILLIS),
      "The connect timeout value in milliseconds for the HttpClient used by the OktaAuthPlugin");
  public static final AwsWrapperProperty SSL_INSECURE = new AwsWrapperProperty("sslInsecure", "false",
      "Whether or not the SSL session is to be secure and the sever's certificates will be verified");
  public static final AwsWrapperProperty DB_USER =
      new AwsWrapperProperty("dbUser", null, "The database user used to access the database");

  public OktaAuthPlugin(PluginService pluginService, CredentialsProviderFactory credentialsProviderFactory) {
    this(pluginService, credentialsProviderFactory, new RdsUtils(), IamAuthUtils.getTokenUtility());
  }

  OktaAuthPlugin(
      final PluginService pluginService,
      final CredentialsProviderFactory credentialsProviderFactory,
      final RdsUtils rdsUtils,
      final IamTokenUtility tokenUtils) {
    super(pluginService, credentialsProviderFactory, rdsUtils, tokenUtils);
    try {
      Class.forName("software.amazon.awssdk.services.sts.model.AssumeRoleWithSamlRequest");
      Class.forName("org.jsoup.nodes.Document");
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(Messages.get("OktaAuthPlugin.requiredDependenciesMissing"));
    }
    this.cacheSizeGauge = this.telemetryFactory.createGauge("oktaAuth.tokenCache.size",
        () -> (long) AuthCacheHolder.tokenCache.size());
    this.fetchTokenCounter = this.telemetryFactory.createCounter("oktaAuth.fetchToken.count");
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
