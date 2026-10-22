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

package software.amazon.jdbc.targetdriverdialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.xa.PGXADataSource;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.states.AuthorizationSessionState;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect.AuthorizationStateImpact;

public class PgTargetDriverDialectTests {
  @Mock private PreparedStatement mockStatement;
  @Mock private Connection mockConnection;
  @Mock private Statement mockJdbcStatement;
  @Mock private ResultSet mockResultSet;
  private final PgTargetDriverDialect dialect = new PgTargetDriverDialect();
  // Held in a field so the logger (and the handler attached to it) cannot be garbage-collected.
  private final Logger converterLogger = Logger.getLogger(TimeoutConverter.class.getName());
  private final List<LogRecord> warnings = Collections.synchronizedList(new ArrayList<>());
  private final Handler warningCollector = new Handler() {
    @Override
    public void publish(final LogRecord record) {
      if (record.getLevel() == Level.WARNING) {
        warnings.add(record);
      }
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
  };
  private Level previousConverterLevel;
  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    previousConverterLevel = converterLogger.getLevel();
    converterLogger.setLevel(Level.ALL);
    converterLogger.addHandler(warningCollector);
    TimeoutConverter.clearReportedTruncatedValues();
  }

  @AfterEach
  void cleanUp() throws Exception {
    converterLogger.removeHandler(warningCollector);
    converterLogger.setLevel(previousConverterLevel);
    TimeoutConverter.clearReportedTruncatedValues();
    closeable.close();
  }

  private static HostSpec host() {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host("h").port(5432).build();
  }

  private static Properties timeouts(final String value) {
    final Properties props = new Properties();
    props.setProperty("loginTimeout", value);
    props.setProperty("connectTimeout", value);
    props.setProperty("socketTimeout", value);
    return props;
  }

  private static Properties socketTimeout(final String value) {
    final Properties props = new Properties();
    props.setProperty("socketTimeout", value);
    return props;
  }

  private void runAllPaths(final Properties props) throws SQLException {
    final Properties connectInfoProps = new Properties();
    connectInfoProps.putAll(props);
    dialect.prepareConnectInfo("jdbc:postgresql://", host(), connectInfoProps);

    final Properties dataSourceProps = new Properties();
    dataSourceProps.putAll(props);
    dialect.prepareDataSource(new PGSimpleDataSource(), "jdbc:postgresql://", host(), dataSourceProps);

    final Properties xaProps = new Properties();
    xaProps.putAll(props);
    dialect.prepareTargetDataSource(new PGXADataSource(), "jdbc:postgresql://h/db", xaProps);
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "999,0", "1000,1", "1500,1", "60000,60"})
  void prepareConnectInfo_convertsTimeoutsToSecondsUnchanged(final String millis, final String seconds) {
    final ConnectInfo connectInfo = dialect.prepareConnectInfo("jdbc:postgresql://", host(), timeouts(millis));

    assertEquals(seconds, connectInfo.props.getProperty("loginTimeout"));
    assertEquals(seconds, connectInfo.props.getProperty("connectTimeout"));
    assertEquals(seconds, connectInfo.props.getProperty("socketTimeout"));
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "999,0", "1000,1", "1500,1", "60000,60"})
  void prepareDataSource_convertsTimeoutsToSecondsUnchanged(final String millis, final int seconds)
      throws SQLException {
    final PGSimpleDataSource dataSource = new PGSimpleDataSource();

    dialect.prepareDataSource(dataSource, "jdbc:postgresql://", host(), timeouts(millis));

    assertEquals(seconds, dataSource.getLoginTimeout());
    assertEquals(seconds, dataSource.getConnectTimeout());
    assertEquals(seconds, dataSource.getSocketTimeout());
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "999,0", "1000,1", "1500,1", "60000,60"})
  void prepareTargetDataSource_xa_convertsTimeoutsToSecondsUnchanged(final String millis, final int seconds) {
    final PGXADataSource dataSource = new PGXADataSource();
    final String url = "jdbc:postgresql://h/db";

    assertSame(url, dialect.prepareTargetDataSource(dataSource, url, timeouts(millis)));
    assertEquals(seconds, dataSource.getLoginTimeout());
    assertEquals(seconds, dataSource.getConnectTimeout());
    assertEquals(seconds, dataSource.getSocketTimeout());
  }

  @Test
  void prepareConnectInfo_leavesUnsetTimeoutsUnset() {
    final ConnectInfo connectInfo = dialect.prepareConnectInfo("jdbc:postgresql://", host(), new Properties());

    assertNull(connectInfo.props.getProperty("loginTimeout"));
    assertNull(connectInfo.props.getProperty("connectTimeout"));
    assertNull(connectInfo.props.getProperty("socketTimeout"));
    assertTrue(warnings.isEmpty());
  }

  @Test
  void warnsOnceForSubSecondValue() throws SQLException {
    dialect.prepareConnectInfo("jdbc:postgresql://", host(), socketTimeout("999"));
    dialect.prepareConnectInfo("jdbc:postgresql://", host(), socketTimeout("999"));
    dialect.prepareDataSource(new PGSimpleDataSource(), "jdbc:postgresql://", host(), socketTimeout("999"));
    dialect.prepareTargetDataSource(new PGXADataSource(), "jdbc:postgresql://h/db", socketTimeout("999"));

    assertEquals(1, warnings.size());
    final String message = warnings.get(0).getMessage();
    assertTrue(message.contains("'socketTimeout'"), message);
    assertTrue(message.contains("999"), message);
    assertTrue(message.contains("pgjdbc"), message);
    assertFalse(message.contains("{"), message);
  }

  @Test
  void warnsPerPropertyAndValue() {
    dialect.prepareConnectInfo("jdbc:postgresql://", host(), timeouts("999"));
    assertEquals(3, warnings.size());

    dialect.prepareConnectInfo("jdbc:postgresql://", host(), socketTimeout("500"));
    assertEquals(4, warnings.size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "1000", "60000"})
  void doesNotWarnForZeroOrWholeSeconds(final String millis) throws SQLException {
    runAllPaths(timeouts(millis));

    assertTrue(warnings.isEmpty());
  }

  @Test
  void testGetQueryFromPreparedStatement() {
    when(mockStatement.toString()).thenReturn("select * from T")
      .thenReturn(" /* delete from User */ delete from users ")
      .thenReturn(null);
    assertEquals("select * from T", dialect.getSQLQueryString(mockStatement));
    assertEquals(" /* delete from User */ delete from users ", dialect.getSQLQueryString(mockStatement));
    assertNull(dialect.getSQLQueryString(mockStatement));
  }

  @Test
  void recognizesSupportedDataSourceClasses() {
    // The PG target driver dialect must recognize all PG data source classes it supports, including
    // the XA data source. If it does not, AwsWrapperXADataSource falls back to the generic dialect,
    // which does not propagate socket/connect timeouts to the target -- breaking failover fast-fail
    // during an XA branch. This must hold for every multi-release variant (base and java24), so this
    // test guards whichever variant matches the running JVM.
    assertTrue(dialect.isDialect("org.postgresql.ds.PGSimpleDataSource"));
    assertTrue(dialect.isDialect("org.postgresql.ds.PGPoolingDataSource"));
    assertTrue(dialect.isDialect("org.postgresql.ds.PGConnectionPoolDataSource"));
    assertTrue(dialect.isDialect("org.postgresql.xa.PGXADataSource"),
        "PG target driver dialect must recognize the PG XA data source");
    assertFalse(dialect.isDialect("com.example.NotPgDataSource"));
  }

  @Test
  void readsAuthorizationSessionState() throws SQLException {
    when(mockConnection.createStatement()).thenReturn(mockJdbcStatement);
    when(mockJdbcStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(1)).thenReturn("application_user");
    when(mockResultSet.getString(2)).thenReturn("tenant_a");
    when(mockResultSet.getString(3)).thenReturn("\"tenant_a\", public");
    when(mockResultSet.getString(4)).thenReturn("[\"pg_catalog\",\"tenant_a\",\"public\"]");

    final Optional<AuthorizationSessionState> result =
        dialect.readAuthorizationSessionState(mockConnection);

    assertEquals(Optional.of(new AuthorizationSessionState(
        "application_user",
        "tenant_a",
        "\"tenant_a\", public",
        "[\"pg_catalog\",\"tenant_a\",\"public\"]")), result);
  }

  @Test
  void returnsEmptyAuthorizationSessionStateWhenDatabaseReturnsNoRow() throws SQLException {
    when(mockConnection.createStatement()).thenReturn(mockJdbcStatement);
    when(mockJdbcStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    assertEquals(Optional.empty(), dialect.readAuthorizationSessionState(mockConnection));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SET ROLE tenant_a",
      "SET SESSION ROLE tenant_a",
      "SET LOCAL ROLE tenant_a",
      "SET SESSION AUTHORIZATION tenant_a",
      "SET SESSION SESSION AUTHORIZATION tenant_a",
      "SET LOCAL SESSION AUTHORIZATION tenant_a",
      "SET LOCAL search_path TO tenant_a, public",
      "SET SCHEMA 'tenant_a'",
      "SET SESSION SCHEMA 'tenant_a'",
      "SET LOCAL SCHEMA 'tenant_a'",
      "SET \"role\" = 'tenant_a'",
      "SET session_authorization = 'tenant_a'",
      "RESET ROLE",
      "RESET \"role\"",
      "RESET SESSION AUTHORIZATION",
      "DISCARD ALL",
      "SELECT 1; /* change tenant */ SET ROLE tenant_a",
      "/* outer /* inner */ still outer */ SET ROLE tenant_a"
  })
  void detectsTrackedAuthorizationStateChanges(final String sql) {
    assertEquals(AuthorizationStateImpact.TRACKED, dialect.getAuthorizationStateImpact(sql));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SET app.tenant_id = 'tenant-a'",
      "SET LOCAL \"app.tenant_id\" TO 'tenant-a'",
      "RESET app.tenant_id",
      "CALL switch_tenant()",
      "DO $$ BEGIN PERFORM set_config('app.tenant_id', 'tenant-a', false); END $$",
      "SELECT set_config('app.tenant_id', 'tenant-a', false)",
      "SELECT pg_catalog.\"set_config\"('search_path', 'tenant_a', false)",
      "CREATE TEMP TABLE tenant_orders (id bigint)",
      "CREATE TEMPORARY TABLE tenant_orders (id bigint)",
      "SELECT * INTO TEMP tenant_orders FROM orders",
      "SELECT * INTO TEMPORARY TABLE tenant_orders FROM orders",
      "CREATE OR REPLACE TEMP VIEW tenant_orders AS SELECT * FROM orders",
      "CREATE TABLE pg_temp.tenant_orders (id bigint)",
      "SELECT * FROM \"pg_temp\".tenant_orders",
      "SELECT * FROM pg_temp_3 . tenant_orders",
      "/* CACHE_PARAM(ttl=600s) */ EXECUTE tenant_stmt",
      "EXPLAIN (ANALYZE, BUFFERS) EXECUTE tenant_stmt",
      "FETCH ALL FROM tenant_cursor"
  })
  void detectsUntrackedAuthorizationStateChanges(final String sql) {
    assertEquals(AuthorizationStateImpact.UNTRACKED, dialect.getAuthorizationStateImpact(sql));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SHOW search_path",
      "SELECT * FROM orders WHERE tenant_id = current_setting('app.tenant_id')",
      "SELECT \"current_setting\"('app.tenant_id')",
      "SELECT currval('orders_id_seq')",
      "SELECT LASTVAL()",
      "SELECT * FROM pg_catalog.pg_prepared_statements",
      "SELECT setting FROM pg_settings WHERE name = 'app.tenant_id'"
  })
  void detectsQueriesThatDependOnUntrackedSessionState(final String sql) {
    assertEquals(AuthorizationStateImpact.UNCACHEABLE, dialect.getAuthorizationStateImpact(sql));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT * FROM orders",
      "SELECT current_user",
      "SELECT 'SET ROLE tenant_a'",
      "PREPARE tenant_stmt AS SELECT * FROM orders",
      "DEALLOCATE tenant_stmt",
      "DEALLOCATE ALL",
      "DECLARE tenant_cursor CURSOR WITH HOLD FOR SELECT * FROM orders",
      "SELECT executed_at, fetch_count FROM jobs",
      "SELECT * FROM my_pg_temp_archive",
      "EXPLAIN SELECT * FROM orders"
  })
  void ignoresStatementsThatDoNotChangeAuthorizationSessionState(final String sql) {
    assertEquals(AuthorizationStateImpact.NONE, dialect.getAuthorizationStateImpact(sql));
  }
}
