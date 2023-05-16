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

public class MysqlConnectorJTargetDriverDialect extends GenericTargetDriverDialect {

  private static final String DRIVER_CLASS_NAME = com.mysql.cj.jdbc.Driver.class.getName();
  private static final String DS_CLASS_NAME = com.mysql.cj.jdbc.MysqlDataSource.class.getName();
  private static final String CP_DS_CLASS_NAME =
      com.mysql.cj.jdbc.MysqlConnectionPoolDataSource.class.getName();

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
    // direct reference to com.mysql.cj.jdbc.MysqlDataSource
    final MysqlConnectorJDataSourceHelper helper = new MysqlConnectorJDataSourceHelper();
    helper.prepareDataSource(dataSource, protocol, hostSpec, props);
  }
}
