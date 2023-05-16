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

import com.mysql.cj.jdbc.MysqlDataSource;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;

public class MysqlConnectorJDataSourceHelper {

  public void prepareDataSource(
      final @NonNull DataSource dataSource,
      final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException {

    if (!(dataSource instanceof MysqlDataSource)) {
      throw new SQLException(Messages.get(
          "TargetDriverDialectManager.unexpectedClass",
          new Object[] {"com.mysql.cj.jdbc.MysqlDataSource", dataSource.getClass().getName()}));
    }

    final MysqlDataSource baseDataSource = (MysqlDataSource) dataSource;

    baseDataSource.setDatabaseName(PropertyDefinition.DATABASE.getString(props));
    baseDataSource.setUser(PropertyDefinition.USER.getString(props));
    baseDataSource.setPassword(PropertyDefinition.PASSWORD.getString(props));
    baseDataSource.setServerName(hostSpec.getHost());

    if (hostSpec.isPortSpecified()) {
      baseDataSource.setPortNumber(hostSpec.getPort());
    }

    // keep unknown properties (the ones that don't belong to AWS Wrapper Driver)
    // and try to apply them to data source
    PropertyDefinition.removeAll(props);
    PropertyUtils.applyProperties(dataSource, props);
  }
}
