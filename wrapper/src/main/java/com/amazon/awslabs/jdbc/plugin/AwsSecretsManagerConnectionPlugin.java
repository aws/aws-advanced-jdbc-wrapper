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

package com.amazon.awslabs.jdbc.plugin;

import static software.amazon.awssdk.regions.Region.regions;

import com.amazon.awslabs.jdbc.HostSpec;
import com.amazon.awslabs.jdbc.JdbcCallable;
import com.amazon.awslabs.jdbc.PropertyDefinition;
import com.amazon.awslabs.jdbc.ProxyDriverProperty;
import com.amazon.awslabs.jdbc.util.StringUtils;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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

public class AwsSecretsManagerConnectionPlugin extends AbstractConnectionPlugin {
  private static final Logger LOGGER = Logger.getLogger(AwsSecretsManagerConnectionPlugin.class.getName());

  protected static final ProxyDriverProperty SECRET_ID_PROPERTY = new ProxyDriverProperty(
      "secretsManagerSecretId", null,
      "The name or the ARN of the secret to retrieve.");
  protected static final ProxyDriverProperty REGION_PROPERTY = new ProxyDriverProperty(
      "secretsManagerRegion", null,
      "The region of the secret to retrieve.");

  private static final String ERROR_MISSING_DEPENDENCY_SECRETS =
      "[AwsSecretsManagerConnectionPlugin] Required dependency 'AWS Java SDK for AWS Secrets Manager' is not on the "
      + "classpath";
  private static final String ERROR_MISSING_DEPENDENCY_JACKSON =
      "[AwsSecretsManagerConnectionPlugin] Required dependency 'Jackson Databind' is not on the classpath";
  static final String ERROR_GET_SECRETS_FAILED =
      "[AwsSecretsManagerConnectionPlugin] Was not able to either fetch or read the database credentials from AWS "
      + "Secrets Manager. Ensure the correct secretId and region properties have been provided";
  static final List<String> SQLSTATE_ACCESS_ERROR = Arrays.asList("28000", "28P01");

  protected static final Map<Pair<String, Region>, Secret> SECRET_CACHE = new ConcurrentHashMap<>();

  private final SecretsManagerClient secretsManagerClient;
  private final GetSecretValueRequest getSecretValueRequest;
  private final Pair<String, Region> secretKey;
  private Secret secret;

  public AwsSecretsManagerConnectionPlugin(Properties props) throws InstantiationException {

    this(
        props,
        null,
        null
    );
  }

  AwsSecretsManagerConnectionPlugin(
      Properties props,
      SecretsManagerClient secretsManagerClient,
      GetSecretValueRequest getSecretValueRequest) throws InstantiationException {

    try {
      Class.forName("software.amazon.awssdk.services.secretsmanager.SecretsManagerClient");
    } catch (ClassNotFoundException e) {
      LOGGER.log(Level.WARNING, ERROR_MISSING_DEPENDENCY_SECRETS);
      throw new InstantiationException(ERROR_MISSING_DEPENDENCY_SECRETS);
    }

    try {
      Class.forName("com.fasterxml.jackson.databind.ObjectMapper");
    } catch (ClassNotFoundException e) {
      LOGGER.log(Level.WARNING, ERROR_MISSING_DEPENDENCY_JACKSON);
      throw new InstantiationException(ERROR_MISSING_DEPENDENCY_JACKSON);
    }

    final String secretId = SECRET_ID_PROPERTY.getString(props);
    if (StringUtils.isNullOrEmpty(secretId)) {
      throw new
          InstantiationException(
              String.format("Configuration parameter '%s' is required.",
              SECRET_ID_PROPERTY.name));
    }

    final String regionString = REGION_PROPERTY.getString(props);
    if (StringUtils.isNullOrEmpty(regionString)) {
      throw new
          InstantiationException(
              String.format("Configuration parameter '%s' is required.",
              REGION_PROPERTY.name));
    }

    final Region region = Region.of(regionString);
    if (!regions().contains(region)) {
      throw new InstantiationException(String.format("Region '%s' is not valid.", regionString));
    }
    this.secretKey = Pair.of(secretId, region);

    if (secretsManagerClient != null && getSecretValueRequest != null) {
      this.secretsManagerClient = secretsManagerClient;
      this.getSecretValueRequest = getSecretValueRequest;

    } else {
      this.secretsManagerClient = SecretsManagerClient.builder()
          .region(region)
          .build();
      this.getSecretValueRequest = GetSecretValueRequest.builder()
          .secretId(secretId)
          .build();
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

    boolean secretWasFetched = updateSecret(false);

    try {
      applySecretToProperties(props);
      return connectFunc.call();

    } catch (SQLException exception) {
      if (isLoginUnsuccessful(exception) && !secretWasFetched) {
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
      LOGGER.log(Level.WARNING, "Unhandled exception: ", exception);
      throw new SQLException(exception);
    }
  }

  /**
   * Called to update credentials from the cache, or from AWS Secrets Manager service.
   *
   * @param forceReFetch Allows ignoring cached credentials and force fetches the latest credentials from the service.
   * @return true, if credentials were fetched from the service.
   */
  private boolean updateSecret(boolean forceReFetch) throws SQLException {

    boolean fetched = false;
    this.secret = SECRET_CACHE.get(this.secretKey);

    if (secret == null || forceReFetch) {
      try {
        this.secret = fetchLatestCredentials();
        if (this.secret != null) {
          fetched = true;
          SECRET_CACHE.put(this.secretKey, this.secret);
        }
      } catch (SecretsManagerException | JsonProcessingException exception) {
        LOGGER.log(Level.WARNING, ERROR_GET_SECRETS_FAILED, exception);
        throw new SQLException(ERROR_GET_SECRETS_FAILED, exception);
      }
    }
    return fetched;
  }

  /**
   * Fetches the current credentials from AWS Secrets Manager service.
   *
   * @return a Secret object containing the credentials fetched from the AWS Secrets Manager service.
   * @throws SecretsManagerException If credentials can't be fetched from AWS Secrets Manager service.
   * @throws JsonProcessingException If credentials can't be mapped to a Secret object.
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

  /**
   * Called to analyse a thrown exception.
   *
   * @param exception Login attempt exception.
   * @return true, if specified exception is caused by unsuccessful login attempt.
   */
  private boolean isLoginUnsuccessful(SQLException exception) {
    LOGGER.log(Level.WARNING, "Login failed. SQLState=" + exception.getSQLState(), exception);
    return SQLSTATE_ACCESS_ERROR.contains(exception.getSQLState());
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
