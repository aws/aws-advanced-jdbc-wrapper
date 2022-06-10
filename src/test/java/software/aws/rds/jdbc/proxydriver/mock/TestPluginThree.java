package software.aws.rds.jdbc.proxydriver.mock;

import software.aws.rds.jdbc.proxydriver.HostSpec;
import software.aws.rds.jdbc.proxydriver.JdbcCallable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;

public class TestPluginThree extends TestPluginOne {

    private Connection connection;

    public TestPluginThree(ArrayList<String> calls) {
        super();
        this.calls = calls;

        this.subscribedMethods = new HashSet<>();
        this.subscribedMethods.add("testJdbcCall_A");
        this.subscribedMethods.add("connect");
    }

    public TestPluginThree(ArrayList<String> calls, Connection connection) {
        this(calls);
        this.connection = connection;
    }

    @Override
    public Connection connect(
            String driverProtocol,
            HostSpec hostSpec,
            Properties props,
            boolean isInitialConnection,
            JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {

        this.calls.add(this.getClass().getSimpleName() + ":before");

        if(this.connection != null) {
            this.calls.add(this.getClass().getSimpleName() + ":connection");
            return this.connection;
        }

        Connection result = connectFunc.call();
        this.calls.add(this.getClass().getSimpleName() + ":after");

        return result;
    }

}
