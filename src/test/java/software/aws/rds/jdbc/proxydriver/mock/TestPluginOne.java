package software.aws.rds.jdbc.proxydriver.mock;

import software.aws.rds.jdbc.proxydriver.ConnectionPlugin;
import software.aws.rds.jdbc.proxydriver.HostSpec;
import software.aws.rds.jdbc.proxydriver.JdbcCallable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class TestPluginOne implements ConnectionPlugin {

    protected Set<String> subscribedMethods;
    protected ArrayList<String> calls;

    TestPluginOne() {
    }

    public TestPluginOne(ArrayList<String> calls) {
        this.calls = calls;

        this.subscribedMethods = new HashSet<>();
        this.subscribedMethods.add("*");
    }

    @Override
    public Set<String> getSubscribedMethods() {
        return this.subscribedMethods;
    }

    @Override
    public <T, E extends Exception> T execute(
            Class<T> resultClass,
            Class<E> exceptionClass,
            Class<?> methodInvokeOn,
            String methodName,
            JdbcCallable<T, E> jdbcMethodFunc,
            Object[] jdbcMethodArgs) throws E {

        this.calls.add(this.getClass().getSimpleName() + ":before");

        T result;
        try {
            result = jdbcMethodFunc.call();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            if (exceptionClass.isInstance(e)) {
                throw exceptionClass.cast(e);
            }
            throw new RuntimeException(e);
        }

        this.calls.add(this.getClass().getSimpleName() + ":after");

        return result;
    }

    @Override
    public Connection connect(
            String driverProtocol,
            HostSpec hostSpec,
            Properties props,
            boolean isInitialConnection,
            JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {

        this.calls.add(this.getClass().getSimpleName() + ":before");
        Connection result = connectFunc.call();
        this.calls.add(this.getClass().getSimpleName() + ":after");
        return result;
    }

    @Override
    public void releaseResources() {
        this.calls.add(this.getClass().getSimpleName());
    }
}
