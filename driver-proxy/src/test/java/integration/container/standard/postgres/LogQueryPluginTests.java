/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration.container.standard.postgres;

import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.junit.jupiter.api.Test;

public class LogQueryPluginTests extends StandardPostgresBaseTest {

  @Test
  public void testStatementExecuteQueryWithArg()
      throws SQLException, UnsupportedEncodingException {

    Logger logger = Logger.getLogger(""); // get root logger
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    StreamHandler handler = new StreamHandler(os, new SimpleFormatter());
    handler.setLevel(Level.ALL);
    logger.addHandler(handler);

    Properties props = initDefaultPropsNoTimeouts();
    props.setProperty("proxyDriverPlugins", "logQuery");
    props.setProperty("enhancedLogQueryEnabled", "true");

    Connection conn = DriverManager.getConnection(getUrl(), props);

    Statement statement = conn.createStatement();

    ResultSet resultSet = statement.executeQuery("SELECT 100");
    resultSet.next();
    resultSet.getInt(1);

    conn.close();

    handler.flush();
    String logMessages = new String(os.toByteArray(), "UTF-8");
    assertTrue(logMessages.contains("[Statement.executeQuery] Executing query: SELECT 100"));
  }

  @Test
  public void testPreparedStatementExecuteQuery()
      throws SQLException, UnsupportedEncodingException {

    Logger logger = Logger.getLogger(""); // get root logger
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    StreamHandler handler = new StreamHandler(os, new SimpleFormatter());
    handler.setLevel(Level.ALL);
    logger.addHandler(handler);

    Properties props = initDefaultPropsNoTimeouts();
    props.setProperty("proxyDriverPlugins", "logQuery");
    props.setProperty("enhancedLogQueryEnabled", "true");

    Connection conn = DriverManager.getConnection(getUrl(), props);

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
