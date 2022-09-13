package integration.container.aurora.mysql.mariadbDriver;

import static org.junit.jupiter.api.Assertions.fail;

import integration.container.aurora.mysql.AuroraMysqlBaseTest;
import java.io.IOException;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.jdbc.Driver;

public abstract class MariadbAuroraMysqlBaseTest extends AuroraMysqlBaseTest {

  protected MariadbAuroraMysqlBaseTest() {
    DB_CONN_STR_PREFIX = "jdbc:aws-wrapper:mariadb://";
  }

  @BeforeAll
  public static void setUpMariadb() throws SQLException, IOException {
    setUp();
    try {
      Class.forName("org.mariadb.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      fail("MariaDB driver not found");
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
