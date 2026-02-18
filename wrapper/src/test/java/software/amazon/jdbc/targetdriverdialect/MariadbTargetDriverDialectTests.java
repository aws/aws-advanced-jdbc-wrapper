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
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
}
