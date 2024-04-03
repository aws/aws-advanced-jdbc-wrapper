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

package integration;

import com.mysql.cj.conf.PropertyKey;
import integration.container.TestDriver;
import integration.container.TestEnvironment;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.PGProperty;
import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;

public class DriverHelper {

  private static final Logger LOGGER = Logger.getLogger(DriverHelper.class.getName());

  public static String getDriverProtocol() {
    return getDriverProtocol(
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
        TestEnvironment.getCurrent().getCurrentDriver());
  }

  public static String getDriverProtocol(DatabaseEngine databaseEngine) {
    switch (databaseEngine) {
      case MYSQL:
        return "jdbc:mysql://";
      case PG:
        return "jdbc:postgresql://";
      case MARIADB:
        return "jdbc:mariadb://";
      default:
        throw new NotImplementedException(databaseEngine.toString());
    }
  }

  public static String getDriverProtocol(DatabaseEngine databaseEngine, TestDriver testDriver) {
    switch (testDriver) {
      case MYSQL:
        return "jdbc:mysql://";
      case PG:
        return "jdbc:postgresql://";
      case MARIADB:
        if (databaseEngine == DatabaseEngine.MARIADB) {
          return "jdbc:mariadb://";
        } else if (databaseEngine == DatabaseEngine.MYSQL) {
          return "jdbc:mysql://";
        }
        throw new UnsupportedOperationException(testDriver.toString());
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }

  public static String getWrapperDriverProtocol() {
    return getWrapperDriverProtocol(
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
        TestEnvironment.getCurrent().getCurrentDriver());
  }

  public static String getWrapperDriverProtocol(TestDriver testDriver) {
    return getWrapperDriverProtocol(
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(), testDriver);
  }

  public static String getWrapperDriverProtocol(
      DatabaseEngine databaseEngine, TestDriver testDriver) {
    switch (testDriver) {
      case MYSQL:
        return "jdbc:aws-wrapper:mysql://";
      case PG:
        return "jdbc:aws-wrapper:postgresql://";
      case MARIADB:
        if (databaseEngine == DatabaseEngine.MARIADB) {
          return "jdbc:aws-wrapper:mariadb://";
        } else if (databaseEngine == DatabaseEngine.MYSQL) {
          return "jdbc:aws-wrapper:mysql://";
        }
        throw new UnsupportedOperationException(testDriver.toString());
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }

  public static String getDriverClassname(DatabaseEngine databaseEngine) {
    switch (databaseEngine) {
      case MYSQL:
        return getDriverClassname(TestDriver.MYSQL);
      case PG:
        return getDriverClassname(TestDriver.PG);
      case MARIADB:
        return getDriverClassname(TestDriver.MARIADB);
      default:
        throw new NotImplementedException(databaseEngine.toString());
    }
  }

  public static String getDriverClassname() {
    return getDriverClassname(TestEnvironment.getCurrent().getCurrentDriver());
  }

  public static String getDriverClassname(TestDriver testDriver) {
    switch (testDriver) {
      case MYSQL:
        return "com.mysql.cj.jdbc.Driver";
      case PG:
        return "org.postgresql.Driver";
      case MARIADB:
        return "org.mariadb.jdbc.Driver";
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }

  public static String getDataSourceClassname() {
    return getDataSourceClassname(TestEnvironment.getCurrent().getCurrentDriver());
  }

  public static String getDataSourceClassname(TestDriver testDriver) {
    switch (testDriver) {
      case MYSQL:
        return "com.mysql.cj.jdbc.MysqlDataSource";
      case PG:
        return "org.postgresql.ds.PGSimpleDataSource";
      case MARIADB:
        return "org.mariadb.jdbc.MariaDbDataSource";
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }

  public static Class<?> getConnectionClass() {
    return getConnectionClass(TestEnvironment.getCurrent().getCurrentDriver());
  }

  public static Class<?> getConnectionClass(TestDriver testDriver) {
    switch (testDriver) {
      case MYSQL:
        return com.mysql.cj.jdbc.ConnectionImpl.class;
      case PG:
        return org.postgresql.PGConnection.class;
      case MARIADB:
        return org.mariadb.jdbc.Connection.class;
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }

  public static String getDriverRequiredParameters() {
    return getDriverRequiredParameters(
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(),
        TestEnvironment.getCurrent().getCurrentDriver());
  }

  public static String getDriverRequiredParameters(TestDriver testDriver) {
    return getDriverRequiredParameters(
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(), testDriver);
  }

  public static String getDriverRequiredParameters(
      DatabaseEngine databaseEngine, TestDriver testDriver) {
    if (testDriver == TestDriver.MARIADB && databaseEngine == DatabaseEngine.MYSQL) {
      return "?permitMysqlScheme";
    }
    return "";
  }

  public static String getHostnameSql() {
    return getHostnameSql(TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine());
  }

  public static String getHostnameSql(DatabaseEngine databaseEngine) {
    switch (databaseEngine) {
      case MYSQL:
      case MARIADB:
        return "SELECT @@hostname";
      case PG:
        return "SELECT inet_server_addr()";
      default:
        throw new NotImplementedException(databaseEngine.toString());
    }
  }

  // This method should be used on connections with a target driver ONLY!
  public static void setConnectTimeout(Properties props, long timeout, TimeUnit timeUnit) {
    setConnectTimeout(TestEnvironment.getCurrent().getCurrentDriver(), props, timeout, timeUnit);
  }

  // This method should be used on connections with a target driver ONLY!
  public static void setConnectTimeout(
      TestDriver testDriver, Properties props, long timeout, TimeUnit timeUnit) {
    switch (testDriver) {
      case MYSQL:
        props.setProperty(
            PropertyKey.connectTimeout.getKeyName(), String.valueOf(timeUnit.toMillis(timeout)));
        break;
      case PG:
        props.setProperty(
            PGProperty.CONNECT_TIMEOUT.getName(), String.valueOf(timeUnit.toSeconds(timeout)));
        break;
      case MARIADB:
        props.setProperty("connectTimeout", String.valueOf(timeUnit.toMillis(timeout)));
        break;
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }

  // This method should be used on connections with a target driver ONLY!
  public static void setSocketTimeout(Properties props, long timeout, TimeUnit timeUnit) {
    setSocketTimeout(TestEnvironment.getCurrent().getCurrentDriver(), props, timeout, timeUnit);
  }

  // This method should be used on connections with a target driver ONLY!
  public static void setSocketTimeout(
      TestDriver testDriver, Properties props, long timeout, TimeUnit timeUnit) {
    switch (testDriver) {
      case MYSQL:
        props.setProperty(
            PropertyKey.socketTimeout.getKeyName(), String.valueOf(timeUnit.toMillis(timeout)));
        break;
      case PG:
        props.setProperty(
            PGProperty.SOCKET_TIMEOUT.getName(), String.valueOf(timeUnit.toSeconds(timeout)));
        break;
      case MARIADB:
        props.setProperty("socketTimeout", String.valueOf(timeUnit.toMillis(timeout)));
        break;
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }

  // This method should be used on connections with a target driver ONLY!
  public static void setTcpKeepAlive(Properties props, boolean enabled) {
    setTcpKeepAlive(TestEnvironment.getCurrent().getCurrentDriver(), props, enabled);
  }

  // This method should be used on connections with a target driver ONLY!
  public static void setTcpKeepAlive(TestDriver testDriver, Properties props, boolean enabled) {
    switch (testDriver) {
      case MYSQL:
        props.setProperty(PropertyKey.tcpKeepAlive.getKeyName(), String.valueOf(enabled));
        break;
      case PG:
        props.setProperty(PGProperty.TCP_KEEP_ALIVE.getName(), String.valueOf(enabled));
        break;
      case MARIADB:
        props.setProperty("tcpKeepAlive", String.valueOf(enabled));
        break;
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }

  // This method should be used on connections with a target driver ONLY!
  public static void setMonitoringConnectTimeout(
      Properties props, long timeout, TimeUnit timeUnit) {
    setMonitoringConnectTimeout(
        TestEnvironment.getCurrent().getCurrentDriver(), props, timeout, timeUnit);
  }

  // This method should be used on connections with a target driver ONLY!
  public static void setMonitoringConnectTimeout(
      TestDriver testDriver, Properties props, long timeout, TimeUnit timeUnit) {
    switch (testDriver) {
      case MYSQL:
        props.setProperty(
            "monitoring-" + PropertyKey.connectTimeout.getKeyName(),
            String.valueOf(timeUnit.toMillis(timeout)));
        break;
      case PG:
        props.setProperty(
            "monitoring-" + PGProperty.CONNECT_TIMEOUT.getName(),
            String.valueOf(timeUnit.toSeconds(timeout)));
        break;
      case MARIADB:
        props.setProperty("monitoring-connectTimeout", String.valueOf(timeUnit.toMillis(timeout)));
        break;
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }

  // This method should be used on connections with a target driver ONLY!
  public static void setMonitoringSocketTimeout(Properties props, long timeout, TimeUnit timeUnit) {
    setMonitoringSocketTimeout(
        TestEnvironment.getCurrent().getCurrentDriver(), props, timeout, timeUnit);
  }

  // This method should be used on connections with a target driver ONLY!
  public static void setMonitoringSocketTimeout(
      TestDriver testDriver, Properties props, long timeout, TimeUnit timeUnit) {
    switch (testDriver) {
      case MYSQL:
        props.setProperty(
            "monitoring-" + PropertyKey.socketTimeout.getKeyName(),
            String.valueOf(timeUnit.toMillis(timeout)));
        break;
      case PG:
        props.setProperty(
            "monitoring-" + PGProperty.SOCKET_TIMEOUT.getName(),
            String.valueOf(timeUnit.toSeconds(timeout)));
        break;
      case MARIADB:
        props.setProperty("monitoring-socketTimeout", String.valueOf(timeUnit.toMillis(timeout)));
        break;
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }

  public static void unregisterAllDrivers() throws SQLException {
    if (software.amazon.jdbc.Driver.isRegistered()) {
      software.amazon.jdbc.Driver.deregister();
    }

    List<Driver> registeredDrivers = Collections.list(DriverManager.getDrivers());
    for (Driver d : registeredDrivers) {
      try {
        DriverManager.deregisterDriver(d);
      } catch (SQLException ex) {
        LOGGER.log(Level.FINEST, "Can't deregister driver " + d.getClass().getName(), ex);
        throw ex;
      }
    }
  }

  public static void registerDriver(DatabaseEngine engine) {
    try {
      Class.forName(DriverHelper.getDriverClassname(engine));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(
          "Driver not found: "
              + DriverHelper.getDriverClassname(engine),
          e);
    }
  }

  public static void registerDriver(TestDriver testDriver) throws SQLException {
    try {
      Driver d = (Driver) Class.forName(getDriverClassname(testDriver)).newInstance();
      DriverManager.registerDriver(d);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Can't register driver.", e);
    }

    if (!software.amazon.jdbc.Driver.isRegistered()) {
      software.amazon.jdbc.Driver.register();
    }

    LOGGER.finest(
        () -> {
          List<Driver> registeredDrivers = Collections.list(DriverManager.getDrivers());
          StringBuilder sb = new StringBuilder("Available drivers: ");
          boolean isFirst = true;
          for (Driver d : registeredDrivers) {
            if (!isFirst) {
              sb.append(", ");
            }
            sb.append(d.getClass().getName());
            isFirst = false;
          }
          return sb.toString();
        });
  }

  public static Connection getDriverConnection(TestEnvironmentInfo info) throws SQLException {
    String url;
    switch (info.getRequest().getDatabaseEngineDeployment()) {
      case AURORA:
      case RDS_MULTI_AZ_CLUSTER:
        url = String.format(
            "%s%s:%d/%s",
            DriverHelper.getDriverProtocol(info.getRequest().getDatabaseEngine()),
            info.getDatabaseInfo().getClusterEndpoint(),
            info.getDatabaseInfo().getClusterEndpointPort(),
            info.getDatabaseInfo().getDefaultDbName());
        break;
      case RDS_MULTI_AZ_INSTANCE:
        url = String.format(
            "%s%s:%d/%s",
            DriverHelper.getDriverProtocol(info.getRequest().getDatabaseEngine()),
            info.getDatabaseInfo().getInstances().get(0).getHost(),
            info.getDatabaseInfo().getInstances().get(0).getPort(),
            info.getDatabaseInfo().getDefaultDbName());
        break;
      default:
        throw new UnsupportedOperationException(info.getRequest().getDatabaseEngineDeployment().toString());
    }
    return DriverManager.getConnection(url, info.getDatabaseInfo().getUsername(), info.getDatabaseInfo().getPassword());
  }
}
