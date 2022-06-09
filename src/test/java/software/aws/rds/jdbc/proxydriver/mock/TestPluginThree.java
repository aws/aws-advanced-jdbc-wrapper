package software.aws.rds.jdbc.proxydriver.mock;

import java.util.ArrayList;
import java.util.HashSet;

public class TestPluginThree extends TestPluginOne {

    public TestPluginThree(ArrayList<String> calls) {
        super();
        this.calls = calls;

        this.subscribedMethods = new HashSet<>();
        this.subscribedMethods.add("testJdbcCall_A");
        this.subscribedMethods.add("openInitialConnection");
    }
}
