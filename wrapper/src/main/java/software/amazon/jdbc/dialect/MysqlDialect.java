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

package software.amazon.jdbc.dialect;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.exceptions.MySQLExceptionHandler;
import software.amazon.jdbc.hostlistprovider.ConnectionStringHostListProvider;
import software.amazon.jdbc.plugin.failover.FailoverRestriction;
import software.amazon.jdbc.util.DriverInfo;

public class MysqlDialect implements Dialect {

  private static final List<String> dialectUpdateCandidates = Arrays.asList(
      DialectCodes.RDS_MULTI_AZ_MYSQL_CLUSTER,
      DialectCodes.AURORA_MYSQL,
      DialectCodes.RDS_MYSQL
  );
  private static MySQLExceptionHandler mySQLExceptionHandler;

  private static final EnumSet<FailoverRestriction> NO_RESTRICTIONS = EnumSet.noneOf(FailoverRestriction.class);

  @Override
  public int getDefaultPort() {
    return 3306;
  }

  @Override
  public ExceptionHandler getExceptionHandler() {
    if (mySQLExceptionHandler == null) {
      mySQLExceptionHandler = new MySQLExceptionHandler();
    }
    return mySQLExceptionHandler;
  }

  @Override
  public String getHostAliasQuery() {
    return "SELECT CONCAT(@@hostname, ':', @@port)";
  }

  @Override
  public String getServerVersionQuery(final Properties properties) {
    return "SHOW VARIABLES LIKE 'version_comment'";
  }

  @Override
  public boolean isDialect(final Connection connection, final Properties properties) {

    // For community Mysql (MysqlDialect):
    // SHOW VARIABLES LIKE 'version_comment'
    // | Variable_name   | value                                            |
    // |-----------------|--------------------------------------------------|
    // | version_comment | MySQL Community Server (GPL) |
    //
    try {
      try (Statement stmt = connection.createStatement();
          ResultSet rs = stmt.executeQuery(this.getServerVersionQuery(properties))) {
        if (!rs.next()) {
          return false;
        }
        String version = rs.getString(2);
        return version != null && version.toLowerCase().contains("mysql");
      }
    } catch (SQLException ex) {
      // do nothing
    }
    return false;
  }

  @Override
  public List<String> getDialectUpdateCandidates() {
    return dialectUpdateCandidates;
  }

  public HostListProviderSupplier getHostListProvider() {
    return (properties, initialUrl, servicesContainer) ->
        new ConnectionStringHostListProvider(properties, initialUrl, servicesContainer.getHostListProviderService());
  }

  @Override
  public void prepareConnectProperties(
      final @NonNull Properties connectProperties,
      final @NonNull String protocol,
      final @NonNull HostSpec hostSpec) {

    final String connectionAttributes = String.format(
        "_jdbc_wrapper_name:aws_jdbc_driver,_jdbc_wrapper_version:%s,_jdbc_wrapper_plugins:%s",
        DriverInfo.DRIVER_VERSION,
        connectProperties.getProperty(ConnectionPluginManager.EFFECTIVE_PLUGIN_CODES_PROPERTY));
    connectProperties.setProperty("connectionAttributes",
        connectProperties.getProperty("connectionAttributes") == null
            ? connectionAttributes
            : connectProperties.getProperty("connectionAttributes") + "," + connectionAttributes);
    connectProperties.remove(ConnectionPluginManager.EFFECTIVE_PLUGIN_CODES_PROPERTY);
  }

  @Override
  public EnumSet<FailoverRestriction> getFailoverRestrictions() {
    return NO_RESTRICTIONS;
  }

  @Override
  public void reportMetadata(@NonNull Connection connection, @NonNull Properties properties) {
    // do nothing
    // Driver metadata has been reported as connection attributes. Check prepareConnectProperties() above.
  }
}
