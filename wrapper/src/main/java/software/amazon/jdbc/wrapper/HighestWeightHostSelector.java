package software.amazon.jdbc.wrapper;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSelector;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.util.Messages;

public class HighestWeightHostSelector implements HostSelector {

  @Override
  public HostSpec getHost(@NonNull final List<HostSpec> hosts,
      @NonNull final HostRole role,
      @Nullable final Properties props) throws SQLException {

    final List<HostSpec> eligibleHosts = hosts.stream()
        .filter(hostSpec ->
            role.equals(hostSpec.getRole()) && hostSpec.getAvailability().equals(HostAvailability.AVAILABLE))
        .collect(Collectors.toList());

    if (eligibleHosts.isEmpty()) {
      throw new SQLException(Messages.get("HostSelector.noHostsMatchingRole", new Object[]{role}));
    }

    return eligibleHosts.stream()
        .max(Comparator.comparing(HostSpec::getWeight))
        .orElseThrow(() -> new SQLException(Messages.get("HostSelector.noHostsMatchingRole", new Object[]{role})));
  }
}
