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

package integration.container.standard.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;
import org.junit.jupiter.api.Test;
import software.aws.jdbc.PropertyDefinition;
import software.aws.jdbc.plugin.ExecutionTimeConnectionPluginFactory;
import software.aws.jdbc.profile.DriverConfigurationProfiles;
import software.aws.jdbc.wrapper.ConnectionWrapper;
import software.aws.jdbc.wrapper.ResultSetWrapper;
import software.aws.jdbc.wrapper.StatementWrapper;

public class StandardMysqlIntegrationTest extends StandardMysqlBaseTest {

  @Test
  public void test_connect() throws SQLException, IOException {
    try (Connection conn = connect()) {
      Statement stmt = conn.createStatement();
      stmt.executeQuery("SELECT 1");
      ResultSet rs = stmt.getResultSet();
      rs.next();
      assertEquals(1, rs.getInt(1));
    }

    try (Connection conn = connectToProxy()) {
      assertTrue(conn.isValid(5));
      containerHelper.disableConnectivity(proxy);
      assertFalse(conn.isValid(5));
      containerHelper.enableConnectivity(proxy);
    }
  }

  @Test
  public void testOpenConnection() throws SQLException {

    Properties props = initDefaultPropsNoTimeouts();
    props.setProperty(PropertyDefinition.PLUGINS.name, "executionTime");

    Connection conn = DriverManager.getConnection(getUrl(), props);

    assertTrue(conn instanceof ConnectionWrapper);
    assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));

    assertTrue(conn.isValid(10));

    Statement statement = conn.createStatement();
    assertNotNull(statement);
    assertTrue(statement instanceof StatementWrapper);
    assertTrue(statement.isWrapperFor(com.mysql.cj.jdbc.StatementImpl.class));

    int rnd = new Random().nextInt(100);
    ResultSet resultSet = statement.executeQuery("SELECT " + rnd);
    assertNotNull(resultSet);
    assertTrue(resultSet instanceof ResultSetWrapper);
    assertTrue(resultSet.isWrapperFor(com.mysql.cj.jdbc.result.ResultSetImpl.class));

    resultSet.next();
    int result = resultSet.getInt(1);
    assertEquals(rnd, result);

    conn.close();
  }

  @Test
  public void testOpenConnectionWithUnknownProfile() {

    Properties props = initDefaultPropsNoTimeouts();
    props.setProperty(PropertyDefinition.PROFILE_NAME.name, "unknownProfile");

    SQLException actualException = assertThrows(SQLException.class, () -> {
      DriverManager.getConnection(getUrl(), props);
    });

    assertTrue(actualException.getMessage().contains("unknownProfile"));
  }

  @Test
  public void testOpenConnectionWithProfile() throws SQLException {

    Properties props = initDefaultPropsNoTimeouts();
    props.setProperty(PropertyDefinition.PROFILE_NAME.name, "testProfile");

    DriverConfigurationProfiles.clear();
    DriverConfigurationProfiles.addOrReplaceProfile("testProfile",
        Arrays.asList(ExecutionTimeConnectionPluginFactory.class));

    Connection conn = DriverManager.getConnection(getUrl(), props);

    assertTrue(conn instanceof ConnectionWrapper);
    assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));

    assertTrue(conn.isValid(10));

    Statement statement = conn.createStatement();
    assertNotNull(statement);
    assertTrue(statement instanceof StatementWrapper);
    assertTrue(statement.isWrapperFor(com.mysql.cj.jdbc.StatementImpl.class));

    int rnd = new Random().nextInt(100);
    ResultSet resultSet = statement.executeQuery("SELECT " + rnd);
    assertNotNull(resultSet);
    assertTrue(resultSet instanceof ResultSetWrapper);
    assertTrue(resultSet.isWrapperFor(com.mysql.cj.jdbc.result.ResultSetImpl.class));

    resultSet.next();
    int result = resultSet.getInt(1);
    assertEquals(rnd, result);

    conn.close();
  }

  @Test
  public void testUnclosedConnection()
      throws SQLException, InterruptedException, UnsupportedEncodingException {

    Logger logger = Logger.getLogger(""); // get root logger
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    logger.addHandler(new StreamHandler(os, new SimpleFormatter()));

    Properties props = initDefaultPropsNoTimeouts();
    props.setProperty(PropertyDefinition.LOG_UNCLOSED_CONNECTIONS.name, "true");

    Connection conn = DriverManager.getConnection(getUrl(), props);

    assertTrue(conn instanceof ConnectionWrapper);
    assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));

    assertTrue(conn.isValid(10));

    Statement statement = conn.createStatement();
    assertNotNull(statement);
    assertTrue(statement instanceof StatementWrapper);
    assertTrue(statement.isWrapperFor(com.mysql.cj.jdbc.StatementImpl.class));

    int rnd = new Random().nextInt(100);
    ResultSet resultSet = statement.executeQuery("SELECT " + rnd);
    assertNotNull(resultSet);
    assertTrue(resultSet instanceof ResultSetWrapper);
    assertTrue(resultSet.isWrapperFor(com.mysql.cj.jdbc.result.ResultSetImpl.class));

    resultSet.next();
    int result = resultSet.getInt(1);
    assertEquals(rnd, result);

    conn = null;

    System.gc();

    Thread.sleep(2000);

    String logMessages = new String(os.toByteArray(), "UTF-8");
    assertTrue(logMessages.contains("Finalizing a connection that was never closed."));
  }

  @Test
  public void testUnclosedConnectionHappyCase()
      throws SQLException, InterruptedException, UnsupportedEncodingException {

    Logger logger = Logger.getLogger(""); // get root logger
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    logger.addHandler(new StreamHandler(os, new SimpleFormatter()));

    Properties props = initDefaultPropsNoTimeouts();
    props.setProperty(PropertyDefinition.LOG_UNCLOSED_CONNECTIONS.name, "true");

    Connection conn = DriverManager.getConnection(getUrl(), props);

    assertTrue(conn instanceof ConnectionWrapper);
    assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));

    assertTrue(conn.isValid(10));

    Statement statement = conn.createStatement();
    assertNotNull(statement);
    assertTrue(statement instanceof StatementWrapper);
    assertTrue(statement.isWrapperFor(com.mysql.cj.jdbc.StatementImpl.class));

    int rnd = new Random().nextInt(100);
    ResultSet resultSet = statement.executeQuery("SELECT " + rnd);
    assertNotNull(resultSet);
    assertTrue(resultSet instanceof ResultSetWrapper);
    assertTrue(resultSet.isWrapperFor(com.mysql.cj.jdbc.result.ResultSetImpl.class));

    resultSet.next();
    int result = resultSet.getInt(1);
    assertEquals(rnd, result);

    conn.close();
    conn = null;

    System.gc();

    Thread.sleep(2000);

    String logMessages = new String(os.toByteArray(), "UTF-8");
    assertFalse(logMessages.contains("Finalizing a connection that was never closed."));
  }
}
