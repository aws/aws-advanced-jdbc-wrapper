package software.aws.rds.jdbc.proxydriver;

import org.junit.jupiter.api.Test;
import software.aws.rds.jdbc.proxydriver.util.TestSettings;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class MysqlTests {

    @Test
    public void testOpenConnection() throws SQLException, ClassNotFoundException {

        // Make sure that MySql driver class is loaded and registered at DriverManager
        Class.forName("com.mysql.cj.jdbc.Driver");

        if(!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
            software.aws.rds.jdbc.proxydriver.Driver.register();
        }

        Properties props = new Properties();
        props.setProperty("user", TestSettings.mysqlUser);
        props.setProperty("password", TestSettings.mysqlPassword);

        Connection conn = DriverManager.getConnection("aws-proxy-jdbc:mysql://" + TestSettings.mysqlServerName + "/", props);

        assertTrue(conn instanceof com.mysql.cj.jdbc.ConnectionImpl);

        conn.close();
    }
}
