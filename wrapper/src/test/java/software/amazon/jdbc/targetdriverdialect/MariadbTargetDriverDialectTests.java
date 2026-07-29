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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.MariaDbDataSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class MariadbTargetDriverDialectTests {
  @Mock private PreparedStatement mockStatement;
  private final MariadbTargetDriverDialect dialect = new MariadbTargetDriverDialect();
  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void testGetQueryFromPreparedStatement() {
    when(mockStatement.toString()).thenReturn("ClientPreparedStatement{sql:'select * from T where A=1', parameters:[]}")
      .thenReturn("ClientPreparedStatement{sql:'/* CACHE_PARAM(ttl=50s) */ select id, title from "
          + "Book b where b.id=1', parameters:[]} ")
      .thenReturn("not a proper response").thenReturn(null);
    assertEquals("'select * from T where A=1', parameters:[]}", dialect.getSQLQueryString(mockStatement));
    assertEquals("'/* CACHE_PARAM(ttl=50s) */ select id, title from Book b where b.id=1', parameters:[]} ",
        dialect.getSQLQueryString(mockStatement));
    assertNull(dialect.getSQLQueryString(mockStatement));
    assertNull(dialect.getSQLQueryString(mockStatement));
  }

  @Test
  void prepareTargetDataSource_addsPermitMysqlSchemeForMysqlUrl() throws SQLException {
    final String url = dialect.prepareTargetDataSource(
        new MariaDbDataSource(), "jdbc:mysql://host:3306/db", new Properties());

    assertEquals("jdbc:mysql://host:3306/db?permitMysqlScheme", url);
  }

  @Test
  void prepareTargetDataSource_keepsExistingQueryStringAndSkipsDuplicates() throws SQLException {
    final String url = dialect.prepareTargetDataSource(
        new MariaDbDataSource(), "jdbc:mysql://host:3306/db?permitMysqlScheme&sslMode=trust", new Properties());

    assertEquals("jdbc:mysql://host:3306/db?permitMysqlScheme&sslMode=trust", url);
  }

  @Test
  void prepareTargetDataSource_leavesMariadbUrlUnchanged() throws SQLException {
    final String url = dialect.prepareTargetDataSource(
        new MariaDbDataSource(), "jdbc:mariadb://host:3306/db", new Properties());

    assertEquals("jdbc:mariadb://host:3306/db", url);
  }

  @Test
  void prepareTargetDataSource_addsTimeoutsInMilliseconds() throws SQLException {
    final Properties props = new Properties();
    props.setProperty("connectTimeout", "10000");
    props.setProperty("socketTimeout", "2000");

    final String url = dialect.prepareTargetDataSource(
        new MariaDbDataSource(), "jdbc:mariadb://host:3306/db", props);

    assertTrue(url.contains("connectTimeout=10000"), url);
    assertTrue(url.contains("socketTimeout=2000"), url);
  }

  @Test
  void prepareTargetDataSource_doesNotOverrideTimeoutsAlreadyInUrl() throws SQLException {
    final Properties props = new Properties();
    props.setProperty("socketTimeout", "2000");

    final String url = dialect.prepareTargetDataSource(
        new MariaDbDataSource(), "jdbc:mariadb://host:3306/db?socketTimeout=500", props);

    assertEquals("jdbc:mariadb://host:3306/db?socketTimeout=500", url);
  }
}
