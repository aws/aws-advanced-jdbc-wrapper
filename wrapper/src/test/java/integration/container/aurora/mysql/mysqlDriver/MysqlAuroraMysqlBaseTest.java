package integration.container.aurora.mysql.mysqlDriver;

import static org.junit.jupiter.api.Assertions.fail;

import integration.container.aurora.mysql.AuroraMysqlBaseTest;
import java.io.IOException;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.jdbc.Driver;

public abstract class MysqlAuroraMysqlBaseTest extends AuroraMysqlBaseTest {
  protected static final String DB_CONN_STR_PREFIX = "jdbc:aws-wrapper:mysql://";
  @BeforeAll
  public static void setUpMysql() throws SQLException, IOException {
    setUp();
    try {
      Class.forName("com.mysql.cj.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      fail("MySQL driver not found");
    }

    if (!Driver.isRegistered()) {
      Driver.register();
    }
  }

  @BeforeEach
  public void setUpEachMysql() throws SQLException, InterruptedException {
    setUpEach(DB_CONN_STR_PREFIX);
  }
}
