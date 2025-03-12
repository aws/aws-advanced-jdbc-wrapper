package software.amazon.jdbc.plugin.bluegreen;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.util.Utils;

public class BlueGreenInterimStatus {
  public BlueGreenPhases blueGreenPhase;
  public String version;
  public int port;
  public List<HostSpec> startTopology;
  public List<HostSpec> currentTopology;
  public Map<String, Optional<String>> startIpAddressesByHostMap;
  public Map<String, Optional<String>> currentIpAddressesByHostMap;
  public Set<String> endpoints; // all known endpoints; just host, no port
  public boolean allStartTopologyIpChanged;
  public boolean allStartTopologyEndpointsRemoved;



  public BlueGreenInterimStatus(
      final BlueGreenPhases blueGreenPhase,
      final String version,
      final int port,
      final List<HostSpec> startTopology,
      final List<HostSpec> currentTopology,
      final Map<String, Optional<String>> startIpAddressesByHostMap,
      final Map<String, Optional<String>> currentIpAddressesByHostMap,
      final Set<String> endpoints,
      boolean allStartTopologyIpChanged,
      boolean allStartTopologyEndpointsRemoved) {

    this.blueGreenPhase = blueGreenPhase;
    this.version = version;
    this.port = port;
    this.startTopology = startTopology;
    this.currentTopology = currentTopology;
    this.startIpAddressesByHostMap = startIpAddressesByHostMap;
    this.currentIpAddressesByHostMap = currentIpAddressesByHostMap;
    this.endpoints = endpoints;
    this.allStartTopologyIpChanged = allStartTopologyIpChanged;
    this.allStartTopologyEndpointsRemoved = allStartTopologyEndpointsRemoved;
  }

  @Override
  public String toString() {
    String currentIpMap = this.currentIpAddressesByHostMap.entrySet().stream()
        .map(x -> String.format("%s -> %s", x.getKey(), x.getValue()))
        .collect(Collectors.joining("\n      "));
    String startIpMap = this.startIpAddressesByHostMap.entrySet().stream()
        .map(x -> String.format("%s -> %s", x.getKey(), x.getValue()))
        .collect(Collectors.joining("\n      "));
    return String.format("%s [\n"
          + "   phase %s, \n"
          + "   %s \n"
          + "   start IP map:\n"
          + "   %s \n"
          + "   current IP map:\n"
          + "   %s \n"
          + "   allStartTopologyIpChanged: %s \n"
          + "   allStartTopologyEndpointsRemoved: %s \n"
          + "]",
        super.toString(), this.blueGreenPhase,
        Utils.logTopology(this.startTopology, "Start topology"),
        startIpMap,
        Utils.logTopology(this.currentTopology, "Current topology"),
        currentIpMap,
        this.allStartTopologyIpChanged,
        this.allStartTopologyEndpointsRemoved);
  }
}
