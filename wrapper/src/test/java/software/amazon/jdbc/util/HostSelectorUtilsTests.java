package software.amazon.jdbc.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.RoundRobinHostSelector;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

public class HostSelectorUtilsTests {
  @Test
  void testSetHostWeightPairsProperty() {
    final String expectedPropertyValue = "instance-1-id:2,instance-2-id:1,instance-3-id:0";

    final List<HostSpec> hosts = Arrays.asList(
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-1")
            .hostId("instance-1-id")
            .weight(2)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-2")
            .hostId("instance-2-id")
            .weight(1)
            .build(),
        new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
            .host("instance-3")
            .hostId("instance-3-id")
            .weight(0)
            .build()
    );
    final Properties properties = new Properties();
    HostSelectorUtils.setHostWeightPairsProperty(RoundRobinHostSelector.ROUND_ROBIN_HOST_WEIGHT_PAIRS, properties, hosts);

    final String actualPropertyValue = properties.getProperty(
        RoundRobinHostSelector.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name);

    assertEquals(expectedPropertyValue, actualPropertyValue);
  }
}
