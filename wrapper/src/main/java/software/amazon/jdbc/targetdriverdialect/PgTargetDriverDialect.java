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

import java.sql.Driver;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.PropertyUtils;

public class PgTargetDriverDialect extends GenericTargetDriverDialect {

  private static final String DRIVER_CLASS_NAME = "org.postgresql.Driver";
  private static final String SIMPLE_DS_CLASS_NAME = "org.postgresql.ds.PGSimpleDataSource";
  private static final String POOLING_DS_CLASS_NAME = "org.postgresql.ds.PGPoolingDataSource";
  private static final String CP_DS_CLASS_NAME = "org.postgresql.ds.PGConnectionPoolDataSource";

  private static final Set<String> dataSourceClassMap = new HashSet<>(Arrays.asList(
      SIMPLE_DS_CLASS_NAME,
      POOLING_DS_CLASS_NAME,
      CP_DS_CLASS_NAME));

  @Override
  public boolean isDialect(Driver driver) {
    return DRIVER_CLASS_NAME.equals(driver.getClass().getName());
  }

  @Override
  public boolean isDialect(String dataSourceClass) {
    return dataSourceClassMap.contains(dataSourceClass);
  }

  @Override
  public ConnectInfo prepareConnectInfo(final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException {

    final String databaseName =
        PropertyDefinition.DATABASE.getString(props) != null
            ? PropertyDefinition.DATABASE.getString(props)
            : "";

    final Boolean tcpKeepAlive = PropertyUtils.getBooleanPropertyValue(props, PropertyDefinition.TCP_KEEP_ALIVE);
    final Integer loginTimeout = PropertyUtils.getIntegerPropertyValue(props, PropertyDefinition.LOGIN_TIMEOUT);
    final Integer connectTimeout = PropertyUtils.getIntegerPropertyValue(props, PropertyDefinition.CONNECT_TIMEOUT);
    final Integer socketTimeout = PropertyUtils.getIntegerPropertyValue(props, PropertyDefinition.SOCKET_TIMEOUT);

    // keep unknown properties (the ones that don't belong to AWS Wrapper Driver)
    // and use them to make a connection
    PropertyDefinition.removeAllExceptCredentials(props);

    if (tcpKeepAlive != null) {
      props.setProperty("tcpKeepAlive", String.valueOf(tcpKeepAlive));
    }

    if (loginTimeout != null) {
      props.setProperty("loginTimeout",
          String.valueOf(TimeUnit.MILLISECONDS.toSeconds(loginTimeout)));
    }
    if (connectTimeout != null) {
      props.setProperty("connectTimeout",
          String.valueOf(TimeUnit.MILLISECONDS.toSeconds(connectTimeout)));
    }
    if (socketTimeout != null) {
      props.setProperty("socketTimeout",
          String.valueOf(TimeUnit.MILLISECONDS.toSeconds(socketTimeout)));
    }

    String urlBuilder = protocol + hostSpec.getUrl() + databaseName;

    return new ConnectInfo(urlBuilder, props);
  }

  @Override
  public void prepareDataSource(
      final @NonNull DataSource dataSource,
      final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException {

    // The logic is isolated to a separated class since it uses
    // direct reference to org.postgresql.ds.common.BaseDataSource
    final PgDataSourceHelper helper = new PgDataSourceHelper();
    helper.prepareDataSource(dataSource, hostSpec, props);
  }
}
