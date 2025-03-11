/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.jdbc.plugin.bluegreen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;

// It should be immutable
public class BlueGreenStatus {

  private final BlueGreenPhases currentPhase;
  private final Map<String, String> hostIpAddresses = new ConcurrentHashMap<>();
  private final Map<String, String> correspondingNodes = new ConcurrentHashMap<>();

  private List<ConnectRouting> connectRouting = new ArrayList<>();
  private List<ExecuteRouting> executeRouting = new ArrayList<>();
  private Map<String, BlueGreenRole> roleByEndpoint = new ConcurrentHashMap<>(); // all known endpoints; host and port
  private final boolean greenNodeChangedName;

  public BlueGreenStatus(final BlueGreenPhases phase) {
    this(phase, new HashMap<>(), new HashMap<>(), false,
        new ArrayList<>(), new ArrayList<>(), new HashMap<>());
  }

  public BlueGreenStatus(
      final BlueGreenPhases phase,
      final Map<String, String> hostIpAddresses,
      final Map<String, String> correspondingNodes,
      final boolean greenNodeChangedName,
      final List<ConnectRouting> connectRouting,
      final List<ExecuteRouting> executeRouting,
      final Map<String, BlueGreenRole> roleByEndpoint) {

    this.currentPhase = phase;
    this.hostIpAddresses.putAll(hostIpAddresses);
    this.correspondingNodes.putAll(correspondingNodes);
    this.greenNodeChangedName = greenNodeChangedName;
    this.connectRouting = connectRouting;
    this.executeRouting = executeRouting;
    this.roleByEndpoint.clear();
    this.roleByEndpoint.putAll(roleByEndpoint);
  }

  public @NonNull BlueGreenPhases getCurrentPhase() {
    return this.currentPhase;
  }

  public @NonNull Map<String, String> getHostIpAddresses() {
    return this.hostIpAddresses;
  }

  public @NonNull Map<String, String> getCorrespondingNodes() {
    return this.correspondingNodes;
  }

  public boolean getGreenNodeChangedName() { return this.greenNodeChangedName; }

  // TODO: make it unmodifiable
  public List<ConnectRouting> getConnectRouting() { return this.connectRouting; }

  // TODO: make it unmodifiable
  public List<ExecuteRouting> getExecuteRouting() { return this.executeRouting; }

  public Map<String, BlueGreenRole> getRoleByEndpoint() { return this.roleByEndpoint; }

  public BlueGreenRole getRole(HostSpec hostSpec) { return this.roleByEndpoint.get(hostSpec.getHostAndPort()); }
}
