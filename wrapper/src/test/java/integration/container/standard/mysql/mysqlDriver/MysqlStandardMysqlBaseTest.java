package integration.container.standard.mysql.mysqlDriver;

import integration.container.standard.mysql.StandardMysqlBaseTest;
import java.io.IOException;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeAll;
import software.amazon.jdbc.Driver;

public class MysqlStandardMysqlBaseTest extends StandardMysqlBaseTest {

  @BeforeAll
  public static void setUpMysql() throws SQLException, IOException, ClassNotFoundException {
    setUp();
    Class.forName("com.mysql.cj.jdbc.Driver");

    if (!Driver.isRegistered()) {
      Driver.register();
    }
  }
}
