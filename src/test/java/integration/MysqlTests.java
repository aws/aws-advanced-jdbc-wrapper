package integration;

import org.junit.jupiter.api.Test;
import integration.util.TestSettings;
import software.aws.rds.jdbc.proxydriver.wrapper.ConnectionWrapper;

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

        Connection conn = DriverManager.getConnection(
                "aws-proxy-jdbc:mysql://" + TestSettings.mysqlServerName + "/" + TestSettings.mysqlDatabase,
                props);

        assertTrue(conn instanceof ConnectionWrapper);
        assertTrue(conn.isWrapperFor(com.mysql.cj.jdbc.ConnectionImpl.class));

        assertTrue(conn.isValid(10));
        conn.close();
    }
}
