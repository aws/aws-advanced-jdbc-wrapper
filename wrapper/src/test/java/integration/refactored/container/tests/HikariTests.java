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

package integration.refactored.container.tests;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariProxyConnection;
import integration.refactored.DriverHelper;
import integration.refactored.TestEnvironmentFeatures;
import integration.refactored.container.ConnectionStringHelper;
import integration.refactored.container.TestDriverProvider;
import integration.refactored.container.TestEnvironment;
import integration.refactored.container.condition.DisableOnTestFeature;
import integration.refactored.container.condition.EnableOnTestFeature;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.HIKARI)
@DisableOnTestFeature(TestEnvironmentFeatures.PERFORMANCE)
public class HikariTests {

  @TestTemplate
  public void testOpenConnectionWithUrl() throws SQLException {

    HikariDataSource ds = new HikariDataSource();
    ds.setJdbcUrl(ConnectionStringHelper.getWrapperUrl());
    ds.setUsername(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    ds.setPassword(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());

    Connection conn = ds.getConnection();

    assertTrue(conn instanceof HikariProxyConnection);
    HikariProxyConnection hikariConn = (HikariProxyConnection) conn;

    assertTrue(hikariConn.isWrapperFor(ConnectionWrapper.class));
    ConnectionWrapper connWrapper = (ConnectionWrapper) hikariConn.unwrap(Connection.class);
    assertTrue(connWrapper.isWrapperFor(DriverHelper.getConnectionClass()));

    assertTrue(conn.isValid(10));
    conn.close();
  }

  @TestTemplate
  public void testOpenConnectionWithDataSourceClassName() throws SQLException {

    HikariDataSource ds = new HikariDataSource();
    ds.setDataSourceClassName(AwsWrapperDataSource.class.getName());

    // Configure the connection pool:
    ds.setUsername(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    ds.setPassword(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());

    // Configure AwsWrapperDataSource:
    ds.addDataSourceProperty("jdbcProtocol", DriverHelper.getDriverProtocol());
    ds.addDataSourceProperty("databasePropertyName", "databaseName");
    ds.addDataSourceProperty("portPropertyName", "port");
    ds.addDataSourceProperty("serverPropertyName", "serverName");

    // Specify the driver-specific DataSource for AwsWrapperDataSource:
    ds.addDataSourceProperty("targetDataSourceClassName", DriverHelper.getDataSourceClassname());

    // Configuring driver-specific DataSource:
    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty(
        "serverName",
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getInstances()
            .get(0)
            .getEndpoint());
    targetDataSourceProps.setProperty(
        "databaseName",
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
    targetDataSourceProps.setProperty("url", ConnectionStringHelper.getUrl());
    ds.addDataSourceProperty("targetDataSourceProperties", targetDataSourceProps);

    Connection conn = ds.getConnection();

    assertTrue(conn instanceof HikariProxyConnection);
    HikariProxyConnection hikariConn = (HikariProxyConnection) conn;

    assertTrue(hikariConn.isWrapperFor(ConnectionWrapper.class));
    ConnectionWrapper connWrapper = (ConnectionWrapper) hikariConn.unwrap(Connection.class);
    assertTrue(connWrapper.isWrapperFor(DriverHelper.getConnectionClass()));

    assertTrue(conn.isValid(10));
    conn.close();
  }
}
