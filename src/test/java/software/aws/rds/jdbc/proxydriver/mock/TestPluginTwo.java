package software.aws.rds.jdbc.proxydriver.mock;

import java.util.ArrayList;
import java.util.HashSet;

public class TestPluginTwo extends TestPluginOne {

    public TestPluginTwo(ArrayList<String> calls) {
        super();
        this.calls = calls;

        this.subscribedMethods = new HashSet<>();
        this.subscribedMethods.add("testJdbcCall_A");
        this.subscribedMethods.add("testJdbcCall_B");
    }
}
