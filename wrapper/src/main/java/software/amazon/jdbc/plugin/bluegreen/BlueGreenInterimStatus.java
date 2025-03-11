package software.amazon.jdbc.plugin.bluegreen;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.util.Utils;

public class BlueGreenInterimStatus {
  public BlueGreenPhases blueGreenPhase;
  public List<HostSpec> topology;
  public Map<String, String> ipAddressesByHostAndPortMap;
  public Set<String> endpoints; // all known endpoints; host and port


  public BlueGreenInterimStatus(
      final BlueGreenPhases blueGreenPhase,
      final List<HostSpec> topology,
      final Map<String, String> ipAddressesByHostAndPortMap,
      final Set<String> endpoints) {
    this.blueGreenPhase = blueGreenPhase;
    this.topology = topology;
    this.ipAddressesByHostAndPortMap = ipAddressesByHostAndPortMap;
    this.endpoints = endpoints;
  }

  @Override
  public String toString() {
    String ipMap = this.ipAddressesByHostAndPortMap.entrySet().stream()
        .map(x -> String.format("%s -> %s", x.getKey(), x.getValue()))
        .collect(Collectors.joining("\n   "));
    return String.format("%s [\nphase %s, \n%s \nIP map:\n   %s\n]",
        super.toString(), this.blueGreenPhase, Utils.logTopology(this.topology), ipMap);
  }
}
