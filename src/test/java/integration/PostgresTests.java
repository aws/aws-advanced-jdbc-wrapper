package integration;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import integration.util.TestSettings;
import software.aws.rds.jdbc.proxydriver.wrapper.ConnectionWrapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
public class PostgresTests {

    @Test
    public void testOpenConnection() throws SQLException {

        if(!org.postgresql.Driver.isRegistered()) {
            org.postgresql.Driver.register();
        }

        if(!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
            software.aws.rds.jdbc.proxydriver.Driver.register();
        }

        Properties props = new Properties();
        props.setProperty("user", TestSettings.postgresqlUser);
        props.setProperty("password", TestSettings.postgresqlPassword);

        Connection conn = DriverManager.getConnection(
                "aws-proxy-jdbc:postgresql://" + TestSettings.postgresqlServerName + "/" + TestSettings.postgresqlDatabase,
                props);

        assertTrue(conn instanceof ConnectionWrapper);
        assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));

        assertTrue(conn.isValid(10));
        conn.close();
    }
}
