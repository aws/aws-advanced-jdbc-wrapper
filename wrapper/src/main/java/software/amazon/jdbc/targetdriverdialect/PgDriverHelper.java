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

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.postgresql.ds.common.BaseDataSource;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;

public class PgDriverHelper {

  private static final Logger LOGGER =
      Logger.getLogger(PgDriverHelper.class.getName());

  private static final String DS_CLASS_NAME =
      org.postgresql.ds.PGSimpleDataSource.class.getName();

  private static final String DS_POOLED_CLASS_NAME =
      org.postgresql.ds.PGConnectionPoolDataSource.class.getName();

  public void prepareDataSource(
      final @NonNull DataSource dataSource,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException {

    if (!(dataSource instanceof BaseDataSource)) {
      throw new SQLException(Messages.get(
          "TargetDriverDialectManager.unexpectedClass",
          new Object[] {DS_CLASS_NAME + ", " + DS_POOLED_CLASS_NAME, dataSource.getClass().getName() }));
    }

    final BaseDataSource baseDataSource = (BaseDataSource) dataSource;

    baseDataSource.setDatabaseName(PropertyDefinition.DATABASE.getString(props));
    baseDataSource.setUser(PropertyDefinition.USER.getString(props));
    baseDataSource.setPassword(PropertyDefinition.PASSWORD.getString(props));
    baseDataSource.setServerNames(new String[] { hostSpec.getHost() });

    if (hostSpec.isPortSpecified()) {
      baseDataSource.setPortNumbers(new int[] { hostSpec.getPort() });
    }

    final Boolean tcpKeepAlive = PropertyUtils.getBooleanPropertyValue(props, PropertyDefinition.TCP_KEEP_ALIVE);
    if (tcpKeepAlive != null) {
      baseDataSource.setTcpKeepAlive(tcpKeepAlive);
    }

    final Integer loginTimeout = PropertyUtils.getIntegerPropertyValue(props, PropertyDefinition.LOGIN_TIMEOUT);
    if (loginTimeout != null) {
      baseDataSource.setLoginTimeout((int) TimeUnit.MILLISECONDS.toSeconds(loginTimeout));
    }

    final Integer connectTimeout = PropertyUtils.getIntegerPropertyValue(props, PropertyDefinition.CONNECT_TIMEOUT);
    if (connectTimeout != null) {
      baseDataSource.setConnectTimeout((int) TimeUnit.MILLISECONDS.toSeconds(connectTimeout));
    }

    final Integer socketTimeout = PropertyUtils.getIntegerPropertyValue(props, PropertyDefinition.SOCKET_TIMEOUT);
    if (socketTimeout != null) {
      baseDataSource.setSocketTimeout((int) TimeUnit.MILLISECONDS.toSeconds(socketTimeout));
    }

    // keep unknown properties (the ones that don't belong to AWS Wrapper Driver)
    // and try to apply them to data source
    PropertyDefinition.removeAll(props);

    PropertyUtils.applyProperties(dataSource, props);
  }

  public boolean isDriverRegistered() throws SQLException {
    return org.postgresql.Driver.isRegistered();
  }

  public void registerDriver() throws SQLException {
    org.postgresql.Driver.register();
  }
}
