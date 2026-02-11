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
}
