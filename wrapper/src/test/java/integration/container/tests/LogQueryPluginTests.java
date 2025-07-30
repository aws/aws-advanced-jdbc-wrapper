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

package integration.container.tests;

import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.condition.DisableOnTestFeature;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.LogQueryConnectionPlugin;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT})
@Order(9)
public class LogQueryPluginTests {

  private static final Logger LOGGER = Logger.getLogger(LogQueryPluginTests.class.getName());

  @TestTemplate
  public void testStatementExecuteQueryWithArg() throws SQLException, UnsupportedEncodingException {

    Logger logger = Logger.getLogger(""); // get root logger
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    StreamHandler handler = new StreamHandler(os, new SimpleFormatter());
    handler.setLevel(Level.ALL);
    logger.addHandler(handler);

    Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "logQuery");
    props.setProperty(LogQueryConnectionPlugin.ENHANCED_LOG_QUERY_ENABLED.name, "true");

    Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);

    Statement statement = conn.createStatement();

    ResultSet resultSet = statement.executeQuery("SELECT 100");
    resultSet.next();
    resultSet.getInt(1);

    conn.close();

    handler.flush();
    String logMessages = new String(os.toByteArray(), "UTF-8");
    assertTrue(logMessages.contains("[Statement.executeQuery] Executing query: SELECT 100"));
  }

  @TestTemplate
  public void testPreparedStatementExecuteQuery()
      throws SQLException, UnsupportedEncodingException {

    Logger logger = Logger.getLogger(""); // get root logger
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    StreamHandler handler = new StreamHandler(os, new SimpleFormatter());
    handler.setLevel(Level.ALL);
    logger.addHandler(handler);

    Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "logQuery");
    props.setProperty(LogQueryConnectionPlugin.ENHANCED_LOG_QUERY_ENABLED.name, "true");

    Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);

    PreparedStatement statement = conn.prepareStatement("SELECT 12345 * ?");
    statement.setInt(1, 10);
    ResultSet resultSet = statement.executeQuery();
    resultSet.next();
    resultSet.getInt(1);

    conn.close();

    handler.flush();
    String logMessages = new String(os.toByteArray(), "UTF-8");
    assertTrue(
        logMessages.contains("[PreparedStatement.executeQuery] Executing query: SELECT 12345 * ?"));
  }
}
