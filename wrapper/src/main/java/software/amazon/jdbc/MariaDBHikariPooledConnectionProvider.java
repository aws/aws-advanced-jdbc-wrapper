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

package software.amazon.jdbc;

import software.amazon.jdbc.util.StringUtils;

public class MariaDBHikariPooledConnectionProvider extends HikariPooledConnectionProvider {
  private static final String DEFAULT_MARIADB_DATA_SOURCE = "com.mysql.cj.jdbc.MysqlDataSource";
  private String dataSourceClassName;

  public MariaDBHikariPooledConnectionProvider(
      HikariPoolConfigurator configurator, String dataSourceClassName) {
    super(configurator);
    this.dataSourceClassName = dataSourceClassName;
  }

  public MariaDBHikariPooledConnectionProvider(HikariPoolConfigurator configurator) {
    super(configurator);
  }

  @Override
  String getDataSourceClassName() {
    return StringUtils.isNullOrEmpty(this.dataSourceClassName)
        ? DEFAULT_MARIADB_DATA_SOURCE : this.dataSourceClassName;
  }
}
