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

package software.amazon.jdbc.plugin;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;
import software.amazon.awssdk.utils.Pair;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

public class AwsSecretsManagerConnectionPlugin extends AbstractConnectionPlugin {
  private static final Logger LOGGER = Logger.getLogger(AwsSecretsManagerConnectionPlugin.class.getName());

  protected static final AwsWrapperProperty SECRET_ID_PROPERTY = new AwsWrapperProperty(
      "secretsManagerSecretId", null,
      "The name or the ARN of the secret to retrieve.");
  protected static final AwsWrapperProperty REGION_PROPERTY = new AwsWrapperProperty(
      "secretsManagerRegion", "us-east-1",
      "The region of the secret to retrieve.");

  protected static final Map<Pair<String, Region>, Secret> secretsCache = new ConcurrentHashMap<>();

  private final Pair<String, Region> secretKey;
  private SecretsManagerClient secretsManagerClient;
  private GetSecretValueRequest getSecretValueRequest;
  private Secret secret;
  protected PluginService pluginService;

  public AwsSecretsManagerConnectionPlugin(PluginService pluginService, Properties props) {

    this(
        pluginService,
        props,
        null,
        null
    );
  }

  AwsSecretsManagerConnectionPlugin(
      PluginService pluginService,
      Properties props,
      SecretsManagerClient secretsManagerClient,
      GetSecretValueRequest getSecretValueRequest) {
    this.pluginService = pluginService;

    try {
      Class.forName("software.amazon.awssdk.services.secretsmanager.SecretsManagerClient");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(Messages.get("AwsSecretsManagerConnectionPlugin.javaSdkNotInClasspath"));
    }

    try {
      Class.forName("com.fasterxml.jackson.databind.ObjectMapper");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(Messages.get("AwsSecretsManagerConnectionPlugin.jacksonDatabindNotInClasspath"));
    }

    final String secretId = SECRET_ID_PROPERTY.getString(props);
    if (StringUtils.isNullOrEmpty(secretId)) {
      throw new
          RuntimeException(
          Messages.get(
              "AwsSecretsManagerConnectionPlugin.missingRequiredConfigParameter",
              new Object[] {SECRET_ID_PROPERTY.name}));
    }

    final String regionString = REGION_PROPERTY.getString(props);
    if (StringUtils.isNullOrEmpty(regionString)) {
      throw new RuntimeException(
          Messages.get(
              "AwsSecretsManagerConnectionPlugin.missingRequiredConfigParameter",
              new Object[] {REGION_PROPERTY.name}));
    }

    final Region region = Region.of(regionString);
    if (!Region.regions().contains(region)) {
      throw new RuntimeException(Messages.get(
          "AwsSecretsManagerConnectionPlugin.unsupportedRegion",
          new Object[] {regionString}));
    }
    this.secretKey = Pair.of(secretId, region);

    if (secretsManagerClient != null && getSecretValueRequest != null) {
      this.secretsManagerClient = secretsManagerClient;
      this.getSecretValueRequest = getSecretValueRequest;
    }
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return new HashSet<>(Collections.singletonList("connect"));
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    if (this.secretsManagerClient == null || this.getSecretValueRequest == null) {
      this.secretsManagerClient = SecretsManagerClient.builder()
          .credentialsProvider(AwsCredentialsManager.getProvider(hostSpec, props))
          .region(this.secretKey.right())
          .build();
      this.getSecretValueRequest = GetSecretValueRequest.builder()
          .secretId(this.secretKey.left())
          .build();
    }

    boolean secretWasFetched = updateSecret(false);

    try {
      applySecretToProperties(props);
      return connectFunc.call();

    } catch (SQLException exception) {
      if (this.pluginService.isLoginException(exception) && !secretWasFetched) {
        // Login unsuccessful with cached credentials
        // Try to re-fetch credentials and try again

        secretWasFetched = updateSecret(true);
        if (secretWasFetched) {
          applySecretToProperties(props);
          return connectFunc.call();
        }
      }

      throw exception;
    } catch (Exception exception) {
      LOGGER.warning(
          () -> Messages.get(
              "AwsSecretsManagerConnectionPlugin.unhandledException",
              new Object[] {exception}));
      throw new SQLException(exception);
    }
  }

  /**
   * Called to update credentials from the cache, or from the AWS Secrets Manager service.
   *
   * @param forceReFetch Allows ignoring cached credentials and force fetches the latest credentials from the service.
   * @return true, if credentials were fetched from the service.
   */
  private boolean updateSecret(boolean forceReFetch) throws SQLException {

    boolean fetched = false;
    this.secret = secretsCache.get(this.secretKey);

    if (secret == null || forceReFetch) {
      try {
        this.secret = fetchLatestCredentials();
        if (this.secret != null) {
          fetched = true;
          secretsCache.put(this.secretKey, this.secret);
        }
      } catch (SecretsManagerException | JsonProcessingException exception) {
        LOGGER.log(
            Level.WARNING,
            exception,
            () -> Messages.get(
                "AwsSecretsManagerConnectionPlugin.failedToFetchDbCredentials"));
        throw new SQLException(Messages.get("AwsSecretsManagerConnectionPlugin.failedToFetchDbCredentials"), exception);
      }
    }
    return fetched;
  }

  /**
   * Fetches the current credentials from AWS Secrets Manager service.
   *
   * @return a Secret object containing the credentials fetched from the AWS Secrets Manager service.
   * @throws SecretsManagerException if credentials can't be fetched from the AWS Secrets Manager service.
   * @throws JsonProcessingException if credentials can't be mapped to a Secret object.
   */
  Secret fetchLatestCredentials() throws SecretsManagerException, JsonProcessingException {
    final GetSecretValueResponse valueResponse = this.secretsManagerClient.getSecretValue(this.getSecretValueRequest);
    final ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(valueResponse.secretString(), Secret.class);
  }

  /**
   * Updates credentials in provided properties. Other plugins in the plugin chain may change them if needed.
   * Eventually, credentials will be used to open a new connection in {@link DefaultConnectionPlugin#connect}.
   *
   * @param properties Properties to store credentials.
   */
  private void applySecretToProperties(Properties properties) {
    if (this.secret != null) {
      PropertyDefinition.USER.set(properties, secret.getUsername());
      PropertyDefinition.PASSWORD.set(properties, secret.getPassword());
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class Secret {
    @JsonProperty("username")
    private String username;
    @JsonProperty("password")
    private String password;

    Secret() {
    }

    Secret(String username, String password) {
      this.username = username;
      this.password = password;
    }

    String getUsername() {
      return this.username;
    }

    String getPassword() {
      return this.password;
    }
  }
}
