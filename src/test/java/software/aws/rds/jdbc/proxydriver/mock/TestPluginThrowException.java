package software.aws.rds.jdbc.proxydriver.mock;

import software.aws.rds.jdbc.proxydriver.HostSpec;
import software.aws.rds.jdbc.proxydriver.JdbcCallable;

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
            JdbcCallable<T, E> executeSqlFunc,
            Object[] args) throws E {

        this.calls.add(this.getClass().getSimpleName() + ":before");
        if(this.isBefore) {
            try {
                throw this.exceptionClass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        T result = executeSqlFunc.call();

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
    public void openInitialConnection(
            HostSpec[] hostSpecs,
            Properties props,
            String url,
            JdbcCallable<Void, Exception> openInitialConnectionFunc) throws Exception {

        this.calls.add(this.getClass().getSimpleName() + ":before");
        if(this.isBefore) {
            try {
                throw this.exceptionClass.newInstance();
            } catch (IllegalAccessException | InstantiationException e) {
                throw new RuntimeException(e);
            }
        }

        openInitialConnectionFunc.call();

        this.calls.add(this.getClass().getSimpleName() + ":after");
        //noinspection ConstantConditions
        if(!this.isBefore) {
            try {
                throw this.exceptionClass.newInstance();
            } catch (IllegalAccessException | InstantiationException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
