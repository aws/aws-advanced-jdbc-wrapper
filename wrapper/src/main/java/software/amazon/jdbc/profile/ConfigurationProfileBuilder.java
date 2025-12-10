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

package software.amazon.jdbc.profile;

import java.util.List;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.ConnectionPluginFactory;
import software.amazon.jdbc.authentication.AwsCredentialsProviderHandler;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

public class ConfigurationProfileBuilder {

  private String name;
  private @Nullable List<Class<? extends ConnectionPluginFactory>> pluginFactories;
  private @Nullable Properties properties;
  private @Nullable Dialect dialect;
  private @Nullable TargetDriverDialect targetDriverDialect;
  private @Nullable ExceptionHandler exceptionHandler;
  private @Nullable AwsCredentialsProviderHandler awsCredentialsProviderHandler;
  private @Nullable ConnectionProvider connectionProvider;

  private ConfigurationProfileBuilder() { }

  public static ConfigurationProfileBuilder get() {
    return new ConfigurationProfileBuilder();
  }

  public ConfigurationProfileBuilder withName(final @NonNull String name) {
    this.name = name;
    return this;
  }

  public ConfigurationProfileBuilder withProperties(final @Nullable Properties properties) {
    this.properties = properties;
    return this;
  }

  public ConfigurationProfileBuilder withPluginFactories(
      final @Nullable List<Class<? extends ConnectionPluginFactory>> pluginFactories) {
    this.pluginFactories = pluginFactories;
    return this;
  }

  public ConfigurationProfileBuilder withDialect(final @Nullable Dialect dialect) {
    this.dialect = dialect;
    return this;
  }

  public ConfigurationProfileBuilder withTargetDriverDialect(
      final @Nullable TargetDriverDialect targetDriverDialect) {
    this.targetDriverDialect = targetDriverDialect;
    return this;
  }

  public ConfigurationProfileBuilder withExceptionHandler(
      final @Nullable ExceptionHandler exceptionHandler) {
    this.exceptionHandler = exceptionHandler;
    return this;
  }

  public ConfigurationProfileBuilder withConnectionProvider(
      final @Nullable ConnectionProvider connectionProvider) {
    this.connectionProvider = connectionProvider;
    return this;
  }

  public ConfigurationProfileBuilder withAwsCredentialsProviderHandler(
      final @Nullable AwsCredentialsProviderHandler awsCredentialsProviderHandler) {
    this.awsCredentialsProviderHandler = awsCredentialsProviderHandler;
    return this;
  }

  public ConfigurationProfileBuilder from(final @NonNull String presetProfileName) {
    final ConfigurationProfile configurationProfile =
        DriverConfigurationProfiles.getProfileConfiguration(presetProfileName);

    if (configurationProfile == null) {
      throw new RuntimeException(Messages.get(
          "Driver.configurationProfileNotFound",
          new Object[] {presetProfileName}));
    }

    this.pluginFactories = configurationProfile.getPluginFactories();
    this.properties = configurationProfile.getProperties();
    this.dialect = configurationProfile.getDialect();
    this.targetDriverDialect = configurationProfile.getTargetDriverDialect();
    this.exceptionHandler = configurationProfile.getExceptionHandler();
    this.connectionProvider = configurationProfile.getConnectionProvider();
    this.awsCredentialsProviderHandler = configurationProfile.getAwsCredentialsProviderHandler();

    return this;
  }

  public ConfigurationProfile build() {
    if (StringUtils.isNullOrEmpty(this.name)) {
      throw new RuntimeException("Profile name is required.");
    }
    if (ConfigurationProfilePresetCodes.isKnownPreset(this.name)) {
      throw new RuntimeException("Can't add or update a built-in preset configuration profile.");
    }

    return new ConfigurationProfile(this.name,
        this.pluginFactories,
        this.properties,
        this.dialect,
        this.targetDriverDialect,
        this.exceptionHandler,
        this.connectionProvider,
        this.awsCredentialsProviderHandler);
  }

  public void buildAndSet() {
    DriverConfigurationProfiles.addOrReplaceProfile(this.name, this.build());
  }
}
