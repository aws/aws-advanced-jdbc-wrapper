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
import software.amazon.jdbc.exceptions.MariaDBExceptionHandler;
import software.amazon.jdbc.hostlistprovider.ConnectionStringHostListProvider;
import software.amazon.jdbc.plugin.failover.FailoverRestriction;
import software.amazon.jdbc.util.DriverInfo;

public class MariaDbDialect implements Dialect {
  private static final List<String> dialectUpdateCandidates = Arrays.asList(
      DialectCodes.AURORA_MYSQL,
      DialectCodes.RDS_MULTI_AZ_MYSQL_CLUSTER,
      DialectCodes.RDS_MYSQL,
      DialectCodes.MYSQL);
  private static MariaDBExceptionHandler mariaDBExceptionHandler;

  private static final EnumSet<FailoverRestriction> NO_RESTRICTIONS = EnumSet.noneOf(FailoverRestriction.class);

  @Override
  public int getDefaultPort() {
    return 3306;
  }

  @Override
  public ExceptionHandler getExceptionHandler() {
    if (mariaDBExceptionHandler == null) {
      mariaDBExceptionHandler = new MariaDBExceptionHandler();
    }
    return mariaDBExceptionHandler;
  }

  @Override
  public String getHostAliasQuery() {
    return "SELECT CONCAT(@@hostname, ':', @@port)";
  }

  @Override
  public String getServerVersionQuery(final Properties properties) {
    return "SELECT 'version', VERSION()";
  }

  @Override
  public boolean isDialect(final Connection connection, final Properties properties) {
    try {
      try (Statement stmt = connection.createStatement();
          ResultSet rs = stmt.executeQuery(this.getServerVersionQuery(properties))) {
        if (!rs.next()) {
          return false;
        }
        String version = rs.getString(2);
        return version != null && version.toLowerCase().contains("mariadb");
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
      final @NonNull Properties connectProperties, final @NonNull String protocol, final @NonNull HostSpec hostSpec) {

    final String connectionAttributes = String.format(
        "_d:aws_jdbc_wrapper,_v:%s,_p:%s",
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
}
