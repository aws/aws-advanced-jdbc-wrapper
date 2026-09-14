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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PgTargetDriverDialectTests {
  @Mock private PreparedStatement mockStatement;
  private final PgTargetDriverDialect dialect = new PgTargetDriverDialect();
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
}
