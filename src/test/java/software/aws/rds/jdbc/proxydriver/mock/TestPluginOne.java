package software.aws.rds.jdbc.proxydriver.mock;

import software.aws.rds.jdbc.proxydriver.ConnectionPlugin;
import software.aws.rds.jdbc.proxydriver.HostSpec;
import software.aws.rds.jdbc.proxydriver.JdbcCallable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

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
            JdbcCallable<T, E> executeSqlFunc,
            Object[] args) throws E {

        this.calls.add(this.getClass().getSimpleName() + ":before");

        T result;
        try {
            result = executeSqlFunc.call();
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
    public void openInitialConnection(
            HostSpec[] hostSpecs,
            Properties props,
            String url,
            JdbcCallable<Void, Exception> openInitialConnectionFunc) throws Exception {

        this.calls.add(this.getClass().getSimpleName() + ":before");
        openInitialConnectionFunc.call();
        this.calls.add(this.getClass().getSimpleName() + ":after");
    }

    @Override
    public void releaseResources() {
        this.calls.add(this.getClass().getSimpleName());
    }
}
