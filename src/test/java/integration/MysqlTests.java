package integration;

import org.junit.jupiter.api.Test;
import integration.util.TestSettings;
import software.aws.rds.jdbc.proxydriver.wrapper.ConnectionWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.ResultSetWrapper;
import software.aws.rds.jdbc.proxydriver.wrapper.StatementWrapper;

import java.sql.*;
import java.util.Properties;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

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

        Statement statement = conn.createStatement();
        assertNotNull(statement);
        assertTrue(statement instanceof StatementWrapper);
        assertTrue(statement.isWrapperFor(com.mysql.cj.jdbc.StatementImpl.class));

        int rnd = new Random().nextInt(100);
        ResultSet resultSet = statement.executeQuery("SELECT " + rnd);
        assertNotNull(resultSet);
        assertTrue(resultSet instanceof ResultSetWrapper);
        assertTrue(resultSet.isWrapperFor(com.mysql.cj.jdbc.result.ResultSetImpl.class));

        resultSet.next();
        int result = resultSet.getInt(1);
        assertEquals(rnd, result);

        conn.close();
    }
}
