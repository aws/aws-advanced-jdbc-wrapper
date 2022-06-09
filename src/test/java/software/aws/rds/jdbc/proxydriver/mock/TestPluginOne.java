package software.aws.rds.jdbc.proxydriver.mock;

import software.aws.rds.jdbc.proxydriver.ConnectionPlugin;
import software.aws.rds.jdbc.proxydriver.HostSpec;

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
    public Object execute(Class<?> methodInvokeOn, String methodName, Callable<?> executeSqlFunc, Object[] args) throws Exception {
        this.calls.add(this.getClass().getSimpleName() + ":before");
        Object result = executeSqlFunc.call();
        this.calls.add(this.getClass().getSimpleName() + ":after");
        return result;
    }

    @Override
    public void openInitialConnection(HostSpec[] hostSpecs, Properties props, String url, Callable<Void> openInitialConnectionFunc)
            throws Exception {
        this.calls.add(this.getClass().getSimpleName() + ":before");
        openInitialConnectionFunc.call();
        this.calls.add(this.getClass().getSimpleName() + ":after");
    }

    @Override
    public void releaseResources() {
        this.calls.add(this.getClass().getSimpleName());
    }
}
