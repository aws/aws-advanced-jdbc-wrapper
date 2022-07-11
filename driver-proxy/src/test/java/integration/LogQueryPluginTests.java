/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration;

import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.util.TestSettings;
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
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Disabled
public class LogQueryPluginTests {

  @BeforeAll
  public static void beforeAll() throws ClassNotFoundException, SQLException {
    Class.forName("com.mysql.cj.jdbc.Driver");

    if (!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
      software.aws.rds.jdbc.proxydriver.Driver.register();
    }

    if (!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
      software.aws.rds.jdbc.proxydriver.Driver.register();
    }
  }

  private static Stream<Arguments> testParameters() {
    return Stream.of(
        Arguments.of(
            "aws-proxy-jdbc:mysql://"
                + TestSettings.mysqlServerName
                + "/"
                + TestSettings.mysqlDatabase,
            TestSettings.mysqlUser,
            TestSettings.mysqlPassword),
        Arguments.of(
            "aws-proxy-jdbc:postgresql://"
                + TestSettings.postgresqlServerName
                + "/"
                + TestSettings.postgresqlDatabase,
            TestSettings.postgresqlUser,
            TestSettings.postgresqlPassword));
  }

  @ParameterizedTest
  @MethodSource("testParameters")
  public void testStatementExecuteQueryWithArg(String url, String user, String password)
      throws SQLException, UnsupportedEncodingException {

    Logger logger = Logger.getLogger(""); // get root logger
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    StreamHandler handler = new StreamHandler(os, new SimpleFormatter());
    handler.setLevel(Level.ALL);
    logger.addHandler(handler);

    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);
    props.setProperty("proxyDriverPlugins", "logQuery");
    props.setProperty("enhancedLogQueryEnabled", "true");

    Connection conn = DriverManager.getConnection(url, props);

    Statement statement = conn.createStatement();

    ResultSet resultSet = statement.executeQuery("SELECT 100");
    resultSet.next();
    resultSet.getInt(1);

    conn.close();

    handler.flush();
    String logMessages = new String(os.toByteArray(), "UTF-8");
    assertTrue(logMessages.contains("[Statement.executeQuery] Executing query: SELECT 100"));
  }

  @ParameterizedTest
  @MethodSource("testParameters")
  public void testPreparedStatementExecuteQuery(String url, String user, String password)
      throws SQLException, UnsupportedEncodingException {

    Logger logger = Logger.getLogger(""); // get root logger
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    StreamHandler handler = new StreamHandler(os, new SimpleFormatter());
    handler.setLevel(Level.ALL);
    logger.addHandler(handler);

    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);
    props.setProperty("proxyDriverPlugins", "logQuery");
    props.setProperty("enhancedLogQueryEnabled", "true");

    Connection conn = DriverManager.getConnection(url, props);

    PreparedStatement statement = conn.prepareStatement("SELECT 12345 * ?");
    statement.setInt(1, 10);
    ResultSet resultSet = statement.executeQuery();
    resultSet.next();
    resultSet.getInt(1);

    conn.close();

    handler.flush();
    String logMessages = new String(os.toByteArray(), "UTF-8");
    assertTrue(logMessages.contains("[PreparedStatement.executeQuery] Executing query: SELECT 12345 * ?"));
  }
}
