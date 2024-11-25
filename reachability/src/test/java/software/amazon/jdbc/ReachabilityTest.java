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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.jdbc.ds.AwsWrapperDataSource;

public class ReachabilityTest {
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";
  private static final String DB = "db";
  private static final String URL = "mydb.cluster-XYZ.us-east-2.rds.amazonaws.com";
  private static final String CUSTOM_ENDPOINT_URL = "mydb.cluster-custom-XYZ.us-east-2.rds.amazonaws.com";
  private static final String DRIVER_PROTOCOL = "jdbc:aws-wrapper:postgresql://";
  private static final String IAM_USERNAME = "john_smith";
  private static final String SECRETS_MANAGER_ID = "my-secret";
  private static final String SECRETS_MANAGER_REGION = "us-east-2";

  private static final String CONN_STRING = DRIVER_PROTOCOL + URL + "/" + DB;
  private static final String CUSTOM_ENDPOINT_CONN_STRING = DRIVER_PROTOCOL + CUSTOM_ENDPOINT_URL + "/" + DB;

  // TODO: federatedAuth, okta, limitless

  @ParameterizedTest
  @MethodSource("getNonSdkPlugins")
  public void testAllNonSdkPlugins(String pluginCode) {
    Properties props = getDefaultProperties();
    props.setProperty("wrapperPlugins", pluginCode);
    connectAndSelect(props);
  }

  static Stream<Arguments> getNonSdkPlugins() {
    return Stream.of(
        Arguments.of("driverMetaData"),
        Arguments.of("dataCache"),
        Arguments.of("initialConnection"),
        Arguments.of("auroraConnectionTracker"),
        Arguments.of("auroraStaleDns"),
        Arguments.of("readWriteSplitting"),
        Arguments.of("failover"),
        Arguments.of("failover2"),
        Arguments.of("efm"),
        Arguments.of("efm2"),
        Arguments.of("fastestResponseStrategy"),
        Arguments.of("logQuery"),
        Arguments.of("connectTime"),
        Arguments.of("executionTime"),
        Arguments.of("dev")
    );
  }

  private Properties getDefaultProperties() {
    Properties props = new Properties();
    props.setProperty("user", USERNAME);
    props.setProperty("password", PASSWORD);
    return props;
  }

  private static void connectAndSelect(Properties props) {
    connectAndSelect(CONN_STRING, props);
  }

  private static void connectAndSelect(String connStr, Properties props) {
    try (Connection conn = DriverManager.getConnection(connStr, props);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT 1")) {
      rs.next();
      assertEquals(1, rs.getInt(1));
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testEnhancedLogQuery() {
    Properties props = getDefaultProperties();
    props.setProperty("enhancedLogQueryEnabled", "true");
    props.setProperty("wrapperPlugins", "logQuery");

    try (Connection conn = DriverManager.getConnection(CONN_STRING, props);
         PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
      stmt.setInt(1, 1);
      ResultSet rs = stmt.executeQuery();
      rs.next();
      assertEquals(1, rs.getInt(1));
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testCustomEndpointPlugin() {
    Properties props = getDefaultProperties();
    props.setProperty("wrapperPlugins", "customEndpoint");
    connectAndSelect(CUSTOM_ENDPOINT_CONN_STRING, props);
  }

  @Test
  public void testIamPlugin() {
    Properties props = new Properties();
    props.setProperty("user", IAM_USERNAME);
    props.setProperty("wrapperPlugins", "iam");
    connectAndSelect(props);
  }

  @Test
  public void testSecretsManagerPlugin() {
    Properties props = new Properties();
    props.setProperty("secretsManagerSecretId", SECRETS_MANAGER_ID);
    props.setProperty("secretsManagerRegion", SECRETS_MANAGER_REGION);
    props.setProperty("wrapperPlugins", "awsSecretsManager");
    connectAndSelect(props);
  }

  @Test
  public void testDataSourceConnection() {
    Properties props = getDefaultProperties();
    props.setProperty("serverName", URL);
    props.setProperty("database", DB);
    props.setProperty("wrapperPlugins", "failover,efm2");

    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(DRIVER_PROTOCOL);
    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");
    ds.setTargetDataSourceProperties(props);

    try (Connection conn = ds.getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT 1")) {
      rs.next();
      assertEquals(1, rs.getInt(1));
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }
}
