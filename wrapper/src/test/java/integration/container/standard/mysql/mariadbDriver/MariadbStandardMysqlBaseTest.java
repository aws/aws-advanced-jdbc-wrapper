package integration.container.standard.mysql.mariadbDriver;

import static org.junit.jupiter.api.Assertions.fail;

import integration.container.standard.mysql.StandardMysqlBaseTest;
import java.io.IOException;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeAll;
import software.amazon.jdbc.Driver;

public class MariadbStandardMysqlBaseTest extends StandardMysqlBaseTest {

  @BeforeAll
  public static void setUpMysql() throws SQLException, IOException, ClassNotFoundException {
    setUp();
    Class.forName("org.mariadb.jdbc.Driver");

    if (!Driver.isRegistered()) {
      Driver.register();
    }
  }
}
