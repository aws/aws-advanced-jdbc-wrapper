package software.amazon.jdbc.util;

import java.util.List;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;

public class HostSelectorUtils {
  public static void setHostWeightPairsProperty(
      final @NonNull AwsWrapperProperty property,
      final @NonNull Properties properties,
      final @NonNull List<HostSpec> hosts) {
    final StringBuilder builder = new StringBuilder();
    for (int i = 0; i < hosts.size(); i++) {
      builder
          .append(hosts.get(i).getHostId())
          .append(":")
          .append(hosts.get(i).getWeight());
      if (i < hosts.size() - 1) {
        builder.append(",");
      }
    }
    final String hostWeightPairsString = builder.toString();
    properties.setProperty(property.name, hostWeightPairsString);
  }
}
