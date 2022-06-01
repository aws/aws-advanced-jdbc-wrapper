package software.aws.rds.jdbc.proxydriver;

import org.junit.jupiter.api.Test;
import software.aws.rds.jdbc.proxydriver.util.TestSettings;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

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

        assertTrue(conn instanceof org.postgresql.PGConnection);

        conn.close();
    }
}
