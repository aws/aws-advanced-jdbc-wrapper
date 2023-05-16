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
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;

public class PgTargetDriverDialect extends GenericTargetDriverDialect {

  private static final String DRIVER_CLASS_NAME = org.postgresql.Driver.class.getName();
  private static final String SIMPLE_DS_CLASS_NAME =
      org.postgresql.ds.PGSimpleDataSource.class.getName();
  private static final String POOLING_DS_CLASS_NAME =
      org.postgresql.ds.PGPoolingDataSource.class.getName();
  private static final String CP_DS_CLASS_NAME =
      org.postgresql.ds.PGConnectionPoolDataSource.class.getName();

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
    // direct reference to org.postgresql.ds.common.BaseDataSource
    final PgDataSourceHelper helper = new PgDataSourceHelper();
    helper.prepareDataSource(dataSource, hostSpec, props);
  }
}
