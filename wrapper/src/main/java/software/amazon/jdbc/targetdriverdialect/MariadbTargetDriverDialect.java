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
import java.util.Properties;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;

public class MariadbTargetDriverDialect extends GenericTargetDriverDialect {

  private static final String PERMIT_MYSQL_SCHEME = "permitMysqlScheme";
  private static final String DRIVER_CLASS_NAME = "org.mariadb.jdbc.Driver";
  private static final String DS_CLASS_NAME = "org.mariadb.jdbc.MariaDbDataSource";
  private static final String CP_DS_CLASS_NAME = "org.mariadb.jdbc.MariaDbPoolDataSource";

  @Override
  public boolean isDialect(Driver driver) {
    return DRIVER_CLASS_NAME.equals(driver.getClass().getName());
  }

  @Override
  public boolean isDialect(String dataSourceClass) {
    return DS_CLASS_NAME.equals(dataSourceClass)
        || CP_DS_CLASS_NAME.equals(dataSourceClass);
  }

  @Override
  public ConnectInfo prepareConnectInfo(final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException {

    final String databaseName =
        PropertyDefinition.DATABASE.getString(props) != null
            ? PropertyDefinition.DATABASE.getString(props)
            : "";
    final boolean permitMysqlSchemeFlag = props.containsKey(PERMIT_MYSQL_SCHEME);

    // "permitMysqlScheme" should be in Url rather than in properties.
    String urlBuilder = protocol + hostSpec.getUrl() + databaseName
        + (permitMysqlSchemeFlag ? "?" + PERMIT_MYSQL_SCHEME : "");

    // keep unknown properties (the ones that don't belong to AWS Wrapper Driver)
    // and use them to make a connection
    props.remove(PERMIT_MYSQL_SCHEME);
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

    // The logic is isolated to a separated class since it uses
    // direct reference to org.mariadb.jdbc.MariaDbDataSource
    final MariadbDataSourceHelper helper = new MariadbDataSourceHelper();
    helper.prepareDataSource(dataSource, protocol, hostSpec, props);
  }
}
