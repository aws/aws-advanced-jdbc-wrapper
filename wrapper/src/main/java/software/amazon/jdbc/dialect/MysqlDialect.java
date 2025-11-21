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
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.exceptions.MySQLExceptionHandler;
import software.amazon.jdbc.hostlistprovider.ConnectionStringHostListProvider;
import software.amazon.jdbc.hostlistprovider.HostListProvider;
import software.amazon.jdbc.plugin.failover.FailoverRestriction;
import software.amazon.jdbc.util.FullServicesContainer;

public class MysqlDialect implements Dialect {

  protected static final String VERSION_QUERY = "SHOW VARIABLES LIKE 'version_comment'";
  protected static final String HOST_ALIAS_QUERY = "SELECT CONCAT(@@hostname, ':', @@port)";

  private static MySQLExceptionHandler mySQLExceptionHandler;
  private static final EnumSet<FailoverRestriction> NO_FAILOVER_RESTRICTIONS =
      EnumSet.noneOf(FailoverRestriction.class);
  private static final List<String> dialectUpdateCandidates = Arrays.asList(
      DialectCodes.GLOBAL_AURORA_MYSQL,
      DialectCodes.AURORA_MYSQL,
      DialectCodes.RDS_MULTI_AZ_MYSQL_CLUSTER,
      DialectCodes.RDS_MYSQL
  );

  protected final DialectUtils dialectUtils = new DialectUtils();

  @Override
  public boolean isDialect(final Connection connection) {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(VERSION_QUERY)) {
      while (rs.next()) {
        final String columnValue = rs.getString(2);
        if (columnValue != null && columnValue.toLowerCase().contains("mysql")) {
          return true;
        }
      }
    } catch (final SQLException ex) {
      return false;
    }

    return false;
  }

  @Override
  public int getDefaultPort() {
    return 3306;
  }

  @Override
  public List<String> getDialectUpdateCandidates() {
    return dialectUpdateCandidates;
  }

  @Override
  public ExceptionHandler getExceptionHandler() {
    if (mySQLExceptionHandler == null) {
      mySQLExceptionHandler = new MySQLExceptionHandler();
    }
    return mySQLExceptionHandler;
  }

  @Override
  public HostListProvider createHostListProvider(
      FullServicesContainer servicesContainer, Properties props, String initialUrl) throws SQLException {
    return new ConnectionStringHostListProvider(props, initialUrl, servicesContainer.getHostListProviderService());
  }

  @Override
  public void prepareConnectProperties(
      final @NonNull Properties connectProperties, final @NonNull String protocol, final @NonNull HostSpec hostSpec) {
    // do nothing
  }

  @Override
  public EnumSet<FailoverRestriction> getFailoverRestrictions() {
    return NO_FAILOVER_RESTRICTIONS;
  }

  @Override
  public String getServerVersionQuery() {
    return VERSION_QUERY;
  }

  @Override
  public String getHostAliasQuery() {
    return HOST_ALIAS_QUERY;
  }
}
