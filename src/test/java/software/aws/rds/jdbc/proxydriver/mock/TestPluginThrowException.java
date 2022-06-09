package software.aws.rds.jdbc.proxydriver.mock;

import software.aws.rds.jdbc.proxydriver.HostSpec;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.Callable;

public class TestPluginThrowException extends TestPluginOne {

    protected Class<? extends Exception> exceptionClass;
    protected boolean isBefore;

    public TestPluginThrowException(ArrayList<String> calls, Class<? extends Exception> exceptionClass, boolean isBefore) {
        super();
        this.calls = calls;
        this.exceptionClass = exceptionClass;
        this.isBefore = isBefore;

        this.subscribedMethods = new HashSet<>();
        this.subscribedMethods.add("*");
    }

    @Override
    public Object execute(Class<?> methodInvokeOn, String methodName, Callable<?> executeSqlFunc, Object[] args) throws Exception {
        this.calls.add(this.getClass().getSimpleName() + ":before");
        if(this.isBefore) {
            throw this.exceptionClass.newInstance();
        }

        Object result = executeSqlFunc.call();

        this.calls.add(this.getClass().getSimpleName() + ":after");
        if(!this.isBefore) {
            throw this.exceptionClass.newInstance();
        }

        return result;
    }

    @Override
    public void openInitialConnection(HostSpec[] hostSpecs, Properties props, String url, Callable<Void> openInitialConnectionFunc) throws Exception {
        this.calls.add(this.getClass().getSimpleName() + ":before");
        if(this.isBefore) {
            throw this.exceptionClass.newInstance();
        }

        openInitialConnectionFunc.call();

        this.calls.add(this.getClass().getSimpleName() + ":after");
        if(!this.isBefore) {
            throw this.exceptionClass.newInstance();
        }
    }
}
