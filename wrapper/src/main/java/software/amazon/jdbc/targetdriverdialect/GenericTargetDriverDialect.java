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

package software.amazon.jdbc.targetdriverdialect;

import static software.amazon.jdbc.util.ConnectionUrlBuilder.buildUrl;
import static software.amazon.jdbc.util.StringUtils.isNullOrEmpty;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.PropertyUtils;

public class GenericTargetDriverDialect implements TargetDriverDialect {

  private static final Logger LOGGER =
      Logger.getLogger(GenericTargetDriverDialect.class.getName());

  @Override
  public boolean isDialect(Driver driver) {
    return true;
  }

  @Override
  public boolean isDialect(String dataSourceClass) {
    return true;
  }

  @Override
  public ConnectInfo prepareConnectInfo(final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException {

    final String databaseName =
        PropertyDefinition.DATABASE.getString(props) != null
            ? PropertyDefinition.DATABASE.getString(props)
            : "";
    String urlBuilder = protocol + hostSpec.getUrl() + databaseName;

    // keep unknown properties (the ones that don't belong to AWS Wrapper Driver)
    // and use them to make a connection
    PropertyDefinition.removeAllExceptCredentials(props);
    return new ConnectInfo(urlBuilder, props);
  }

  @Override
  public void prepareDataSource(
      final @NonNull DataSource dataSource,
      final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final @Nullable String serverPropertyName,
      final @Nullable String portPropertyName,
      final @Nullable String urlPropertyName,
      final @Nullable String databasePropertyName) throws SQLException {

    final Properties customProps = new Properties();

    if (!isNullOrEmpty(serverPropertyName)) {
      customProps.setProperty(serverPropertyName, hostSpec.getHost());
    }

    if (hostSpec.isPortSpecified() && !isNullOrEmpty(portPropertyName)) {
      customProps.put(portPropertyName, hostSpec.getPort());
    }

    if (!isNullOrEmpty(databasePropertyName)
        && !isNullOrEmpty(PropertyDefinition.DATABASE.getString(props))) {
      customProps.setProperty(databasePropertyName, PropertyDefinition.DATABASE.getString(props));
    }

    if (!isNullOrEmpty(urlPropertyName)) {
      final Properties urlProperties = PropertyUtils.copyProperties(customProps);

      if (!isNullOrEmpty(props.getProperty(urlPropertyName))) {
        // Remove the current url property to replace with a new url built from updated HostSpec and properties
        urlProperties.remove(urlPropertyName);
      }

      String finalUrl = buildUrl(
          protocol,
          hostSpec,
          serverPropertyName,
          portPropertyName,
          databasePropertyName,
          urlProperties);
      LOGGER.finest(() -> "Connecting to " + finalUrl);
      customProps.setProperty(urlPropertyName, finalUrl);
    }

    LOGGER.finest(() -> PropertyUtils.logProperties(customProps, "Connecting with properties: \n"));

    if (!customProps.isEmpty()) {
      PropertyUtils.applyProperties(dataSource, customProps);
    }
  }

}
