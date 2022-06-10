package software.aws.rds.jdbc.proxydriver.mock;

import software.aws.rds.jdbc.proxydriver.HostSpec;
import software.aws.rds.jdbc.proxydriver.JdbcCallable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;

public class TestPluginThrowException extends TestPluginOne {

    protected final Class<? extends Exception> exceptionClass;
    protected final boolean isBefore;

    public TestPluginThrowException(ArrayList<String> calls, Class<? extends Exception> exceptionClass, boolean isBefore) {
        super();
        this.calls = calls;
        this.exceptionClass = exceptionClass;
        this.isBefore = isBefore;

        this.subscribedMethods = new HashSet<>();
        this.subscribedMethods.add("*");
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
        if(this.isBefore) {
            try {
                throw this.exceptionClass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        T result = jdbcMethodFunc.call();

        this.calls.add(this.getClass().getSimpleName() + ":after");
        //noinspection ConstantConditions
        if(!this.isBefore) {
            try {
                throw this.exceptionClass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

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
        if(this.isBefore) {
            throwException();
        }

        Connection conn = connectFunc.call();

        this.calls.add(this.getClass().getSimpleName() + ":after");
        if(!this.isBefore) {
            throwException();
        }

        return conn;
    }

    private void throwException() throws SQLException {
        try {
            throw this.exceptionClass.newInstance();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            if(e instanceof SQLException) {
                throw (SQLException) e;
            }
            throw new SQLException(e);
        }
    }
}
