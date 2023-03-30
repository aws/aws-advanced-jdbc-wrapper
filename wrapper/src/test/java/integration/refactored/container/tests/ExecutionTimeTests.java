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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.refactored.DriverHelper;
import integration.refactored.TestEnvironmentFeatures;
import integration.refactored.container.ConnectionStringHelper;
import integration.refactored.container.TestDriverProvider;
import integration.refactored.container.condition.DisableOnTestFeature;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.wrapper.ConnectionWrapper;
import software.amazon.jdbc.wrapper.ResultSetWrapper;
import software.amazon.jdbc.wrapper.StatementWrapper;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@DisableOnTestFeature({TestEnvironmentFeatures.PERFORMANCE, TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY})
public class ExecutionTimeTests {

  private static final Logger LOGGER = Logger.getLogger(ExecutionTimeTests.class.getName());

  @TestTemplate
  public void testExecutionTime() throws SQLException, UnsupportedEncodingException {

    Logger logger = Logger.getLogger(""); // get root logger
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    StreamHandler handler = new StreamHandler(os, new SimpleFormatter());
    handler.setLevel(Level.ALL);
    logger.addHandler(handler);

    Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "executionTime");

    Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);

    assertTrue(conn instanceof ConnectionWrapper);
    assertTrue(conn.isWrapperFor(DriverHelper.getConnectionClass()));

    assertTrue(conn.isValid(10));

    Statement statement = conn.createStatement();
    assertNotNull(statement);
    assertTrue(statement instanceof StatementWrapper);

    int rnd = new Random().nextInt(100);
    ResultSet resultSet = statement.executeQuery("SELECT " + rnd);
    assertNotNull(resultSet);
    assertTrue(resultSet instanceof ResultSetWrapper);

    resultSet.next();
    int result = resultSet.getInt(1);
    assertEquals(rnd, result);

    conn.close();

    handler.flush();
    String logMessages = new String(os.toByteArray(), "UTF-8");
    LOGGER.finest("logMessages=" + logMessages);

    assertTrue(logMessages.contains("Executed Statement.executeQuery in"));
  }
}
