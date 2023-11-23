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

import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;
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
      final @NonNull Properties props) throws SQLException {

    String finalUrl = buildUrl(
        protocol,
        hostSpec.getHost(),
        hostSpec.getPort(),
        PropertyDefinition.DATABASE.getString(props));

    LOGGER.finest(() -> "Connecting to " + finalUrl);
    props.setProperty("url", finalUrl);

    PropertyDefinition.removeAllExceptCredentials(props);
    LOGGER.finest(() -> PropertyUtils.logProperties(PropertyUtils.maskProperties(props),
        "Connecting with properties: \n"));

    if (!props.isEmpty()) {
      PropertyUtils.applyProperties(dataSource, props);
    }
  }

  public boolean isDriverRegistered() throws SQLException {
    throw new SQLException(Messages.get("TargetDriverDialect.unsupported"));
  }

  public void registerDriver() throws SQLException {
    throw new SQLException(Messages.get("TargetDriverDialect.unsupported"));
  }
}
