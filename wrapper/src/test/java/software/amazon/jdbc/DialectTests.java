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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.dialect.AuroraMysqlDialect;
import software.amazon.jdbc.dialect.AuroraPgDialect;
import software.amazon.jdbc.dialect.DialectManager;
import software.amazon.jdbc.dialect.MariaDbDialect;
import software.amazon.jdbc.dialect.MysqlDialect;
import software.amazon.jdbc.dialect.PgDialect;
import software.amazon.jdbc.dialect.RdsMultiAzDbClusterMysqlDialect;
import software.amazon.jdbc.dialect.RdsMultiAzDbClusterPgDialect;
import software.amazon.jdbc.dialect.RdsMysqlDialect;
import software.amazon.jdbc.dialect.RdsPgDialect;

public class DialectTests {
  @Mock private Connection mockConnection;
  @Mock private Statement mockStatement;
  @Mock private ResultSet successResultSet;
  @Mock private ResultSet failResultSet;
  @Mock private ResultSetMetaData mockResultSetMetaData;
  private final MysqlDialect mysqlDialect = new MysqlDialect();
  private final RdsMysqlDialect rdsMysqlDialect = new RdsMysqlDialect();
  private final RdsMultiAzDbClusterMysqlDialect rdsTazMysqlDialect = new RdsMultiAzDbClusterMysqlDialect();
  private final AuroraMysqlDialect auroraMysqlDialect = new AuroraMysqlDialect();
  private final PgDialect pgDialect = new PgDialect();
  private final RdsPgDialect rdsPgDialect = new RdsPgDialect();
  private final RdsMultiAzDbClusterPgDialect rdsTazPgDialect = new RdsMultiAzDbClusterPgDialect();
  private final AuroraPgDialect auroraPgDialect = new AuroraPgDialect();
  private final MariaDbDialect mariaDbDialect = new MariaDbDialect();
  private AutoCloseable closeable;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    when(this.mockConnection.createStatement()).thenReturn(this.mockStatement);
    when(this.failResultSet.next()).thenReturn(false);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  // MYSQL DIALECT
  @Test
  void testMysqlIsDialectSuccess() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(successResultSet);
    when(successResultSet.next()).thenReturn(true);
    when(successResultSet.getString(1)).thenReturn("MySQL Community Server (GPL)");
    when(successResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(1);
    assertTrue(mysqlDialect.isDialect(mockConnection));
  }

  @Test
  void testMysqlIsDialectQueryReturnedEmptyResultSet() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(failResultSet);
    assertFalse(mysqlDialect.isDialect(mockConnection));
    verify(failResultSet, times(1)).next();
  }

  @Test
  void testMysqlIsDialectExceptionThrown() throws SQLException {
    when(mockStatement.executeQuery(any())).thenThrow(new SQLException());
    assertFalse(mysqlDialect.isDialect(mockConnection));
  }

  @Test
  void testMysqlIsDialectIncorrectVersionComment() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(successResultSet);
    when(successResultSet.next()).thenReturn(true, false);
    when(successResultSet.getString(1)).thenReturn("Invalid");
    when(successResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(1);
    assertFalse(mysqlDialect.isDialect(mockConnection));
  }

  // RDS MYSQL DIALECT
  @Test
  void testRdsMysqlIsDialectSuccess() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(successResultSet);
    when(successResultSet.next()).thenReturn(true, false, true, true);
    when(successResultSet.getString(1)).thenReturn("Source distribution");
    when(successResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(1);
    assertTrue(rdsMysqlDialect.isDialect(mockConnection));
  }

  @Test
  void testRdsMysqlIsDialectQueryReturnedEmptyResultSet() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(failResultSet);
    assertFalse(rdsMysqlDialect.isDialect(mockConnection));
    verify(failResultSet, times(2)).next(); // once for super.isDialect()
  }

  @Test
  void testRdsMysqlIsDialectExceptionThrown() throws SQLException {
    when(mockStatement.executeQuery(any())).thenThrow(new SQLException());
    assertFalse(rdsMysqlDialect.isDialect(mockConnection));
  }

  @Test
  void testRdsMysqlIsDialectSuperIsDialectReturnedTrue() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(successResultSet);
    when(successResultSet.next()).thenReturn(true, false);
    when(successResultSet.getString(1)).thenReturn("MySQL Community Server (GPL)");
    when(successResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(1);
    assertFalse(rdsMysqlDialect.isDialect(mockConnection));
    verify(successResultSet, times(1)).next();
  }

  @Test
  void testRdsMysqlIsDialectInvalidVersionComment() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(successResultSet);
    when(successResultSet.next()).thenReturn(true, false, true, false);
    when(successResultSet.getString(1)).thenReturn("Invalid");
    when(successResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(1);
    assertFalse(rdsMysqlDialect.isDialect(mockConnection));
    verify(successResultSet, times(4)).next();
  }

  // RDS MULTI A-Z DB CLUSTER MYSQL DIALECT
  @Test
  void testRdsTazMysqlIsDialectSuccess() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(successResultSet);
    when(successResultSet.next()).thenReturn(true, true, true);
    when(successResultSet.getString(2)).thenReturn("any-ip-address");
    when(successResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(2);
    assertTrue(rdsTazMysqlDialect.isDialect(mockConnection));
  }

  @Test
  void testRdsTazMysqlIsDialectExceptionThrown() throws SQLException {
    when(mockStatement.executeQuery(any())).thenThrow(new SQLException());
    assertFalse(rdsTazMysqlDialect.isDialect(mockConnection));
  }

  @Test
  void testRdsTazMysqlIsDialectQueryReturnedEmptyResultSet() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(failResultSet);
    assertFalse(rdsTazMysqlDialect.isDialect(mockConnection));
    verify(failResultSet, times(1)).next();
  }

  // AURORA MYSQL DIALECT
  @Test
  void testAuroraMysqlIsDialectSuccess() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(successResultSet);
    when(successResultSet.next()).thenReturn(true);
    assertTrue(auroraMysqlDialect.isDialect(mockConnection));
  }

  @Test
  void testAuroraMysqlIsDialectExceptionThrown() throws SQLException {
    when(mockStatement.executeQuery(any())).thenThrow(new SQLException());
    assertFalse(auroraMysqlDialect.isDialect(mockConnection));
  }

  @Test
  void testAuroraMysqlIsDialectQueryReturnedEmptyResultSet() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(failResultSet);
    assertFalse(auroraMysqlDialect.isDialect(mockConnection));
    verify(failResultSet, times(1)).next();
  }

  // PG DIALECT
  @Test
  void testPgIsDialectSuccess() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(successResultSet);
    when(successResultSet.next()).thenReturn(true);
    assertTrue(pgDialect.isDialect(mockConnection));
  }

  @Test
  void testPgIsDialectExceptionThrown() throws SQLException {
    when(mockStatement.executeQuery(any())).thenThrow(new SQLException());
    assertFalse(pgDialect.isDialect(mockConnection));
  }

  @Test
  void testPgIsDialectQueryReturnedEmptyResultSet() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(failResultSet);
    assertFalse(pgDialect.isDialect(mockConnection));
    verify(failResultSet, times(1)).next();
  }

  // RDS PG DIALECT
  @Test
  void testRdsPgIsDialectSuccess() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(successResultSet);
    when(successResultSet.next()).thenReturn(true);
    when(successResultSet.getBoolean("rds_tools")).thenReturn(true);
    when(successResultSet.getBoolean("aurora_stat_utils")).thenReturn(false);
    assertTrue(rdsPgDialect.isDialect(mockConnection));
  }

  @Test
  void testRdsPgIsDialectExceptionThrown() throws SQLException {
    when(mockStatement.executeQuery(any())).thenThrow(new SQLException());
    assertFalse(rdsPgDialect.isDialect(mockConnection));
  }

  @Test
  void testRdsPgIsDialectQueryReturnedEmptyResultSet() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(failResultSet);
    assertFalse(rdsPgDialect.isDialect(mockConnection));
    verify(failResultSet, times(1)).next();
  }

  @Test
  void testRdsPgIsDialectIsAurora() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(successResultSet);
    when(successResultSet.next()).thenReturn(true, false);
    when(successResultSet.getBoolean("rds_tools")).thenReturn(true);
    when(successResultSet.getBoolean("aurora_stat_utils")).thenReturn(true);
    assertFalse(rdsPgDialect.isDialect(mockConnection));
  }

  // RDS MULTI A-Z DB CLUSTER PG DIALECT
  @Test
  void testRdsTazPgIsDialectSuccess() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(successResultSet);
    when(successResultSet.next()).thenReturn(true);
    assertTrue(rdsTazPgDialect.isDialect(mockConnection));
  }

  @Test
  void testRdsTazPgIsDialectExceptionThrown() throws SQLException {
    when(mockStatement.executeQuery(any())).thenThrow(new SQLException());
    assertFalse(rdsTazPgDialect.isDialect(mockConnection));
  }

  @Test
  void testRdsTazPgIsDialectQueryReturnedEmptyResultSet() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(failResultSet);
    assertFalse(rdsTazPgDialect.isDialect(mockConnection));
    verify(failResultSet, times(1)).next();
  }

  @Test
  void testRdsTazPgIsDialectWriterNodeQueryFailed() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(successResultSet, failResultSet);
    when(successResultSet.next()).thenReturn(true);
    assertFalse(rdsTazPgDialect.isDialect(mockConnection));
    verify(successResultSet, times(1)).next();
    verify(failResultSet, times(1)).next();
  }

  // AURORA PG DIALECT
  @Test
  void testAuroraPgIsDialectSuccess() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(successResultSet);
    when(successResultSet.next()).thenReturn(true);
    when(successResultSet.getBoolean("aurora_stat_utils")).thenReturn(true);
    assertTrue(auroraPgDialect.isDialect(mockConnection));
  }

  @Test
  void testAuroraPgIsDialectExceptionThrown() throws SQLException {
    when(mockStatement.executeQuery(any())).thenThrow(new SQLException());
    assertFalse(auroraPgDialect.isDialect(mockConnection));
  }

  @Test
  void testAuroraPgIsDialectQueryReturnedEmptyResultSet() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(failResultSet);
    assertFalse(auroraPgDialect.isDialect(mockConnection));
    verify(failResultSet, times(1)).next();
  }

  @Test
  void testAuroraPgIsDialectMissingAuroraStatUtils() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(successResultSet);
    when(successResultSet.next()).thenReturn(true);
    when(successResultSet.getBoolean("aurora_stat_utils")).thenReturn(false);
    assertFalse(auroraPgDialect.isDialect(mockConnection));
  }

  // MARIADB DIALECT
  @Test
  void testMariaDbIsDialectSuccess() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(successResultSet);
    when(successResultSet.next()).thenReturn(true);
    when(successResultSet.getString(1)).thenReturn("mariadb");
    assertTrue(mariaDbDialect.isDialect(mockConnection));
  }

  @Test
  void testMariaDbIsDialectExceptionThrown() throws SQLException {
    when(mockStatement.executeQuery(any())).thenThrow(new SQLException());
    assertFalse(mariaDbDialect.isDialect(mockConnection));
  }

  @Test
  void testMariaDbIsDialectQueryReturnedEmptyResultSet() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(failResultSet);
    assertFalse(mariaDbDialect.isDialect(mockConnection));
    verify(failResultSet, times(1)).next();
  }

  @Test
  void testMariaDbIsDialectIncorrectVersion() throws SQLException {
    when(mockStatement.executeQuery(any())).thenReturn(successResultSet);
    when(successResultSet.next()).thenReturn(true, false);
    when(successResultSet.getString(1)).thenReturn("Invalid");
    assertFalse(mariaDbDialect.isDialect(mockConnection));
  }
}
