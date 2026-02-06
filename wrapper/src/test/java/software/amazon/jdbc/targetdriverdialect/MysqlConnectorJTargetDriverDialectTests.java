package software.amazon.jdbc.targetdriverdialect;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import java.sql.PreparedStatement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

public class MysqlConnectorJTargetDriverDialectTests {
  @Mock private PreparedStatement mockStatement;
  private final MysqlConnectorJTargetDriverDialect dialect = new MysqlConnectorJTargetDriverDialect();
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
    when(mockStatement.toString()).thenReturn("com.mysql.cj.jdbc.ClientPreparedStatement: select * from T where A=1")
      .thenReturn("com.mysql.cj.jdbc.ClientPreparedStatement: /* CACHE_PARAM(ttl=50s) */ select book0_.id as id1, book0_.title as title2 from Book book0_ where book0_.id=1 ")
      .thenReturn("not a proper response")
      .thenReturn(null);
    assertEquals(" select * from T where A=1", dialect.getSQLQueryString(mockStatement));
    assertEquals(" /* CACHE_PARAM(ttl=50s) */ select book0_.id as id1, book0_.title as title2 from Book book0_ where book0_.id=1 ", dialect.getSQLQueryString(mockStatement));
    assertNull(dialect.getSQLQueryString(mockStatement));
    assertNull(dialect.getSQLQueryString(mockStatement));
  }
}
