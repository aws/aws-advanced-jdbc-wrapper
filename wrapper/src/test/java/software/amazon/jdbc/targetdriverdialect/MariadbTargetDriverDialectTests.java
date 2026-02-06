package software.amazon.jdbc.targetdriverdialect;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import java.sql.PreparedStatement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

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
      .thenReturn("ClientPreparedStatement{sql:'/* CACHE_PARAM(ttl=50s) */ select id, title from Book b where b.id=1', parameters:[]} ")
      .thenReturn("not a proper response").thenReturn(null);
    assertEquals("'select * from T where A=1', parameters:[]}", dialect.getSQLQueryString(mockStatement));
    assertEquals("'/* CACHE_PARAM(ttl=50s) */ select id, title from Book b where b.id=1', parameters:[]} ", dialect.getSQLQueryString(mockStatement));
    assertNull(dialect.getSQLQueryString(mockStatement));
    assertNull(dialect.getSQLQueryString(mockStatement));
  }
}
