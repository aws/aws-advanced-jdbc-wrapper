package software.aws.rds.jdbc.proxydriver.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public class TestPluginTwo extends TestPluginOne {

  public TestPluginTwo(ArrayList<String> calls) {
    super();
    this.calls = calls;

    this.subscribedMethods = new HashSet<>(Arrays.asList("testJdbcCall_A", "testJdbcCall_B"));
  }
}
