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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.checkerframework.checker.nullness.qual.NonNull;

public class BlueGreenStatus {

  private final BlueGreenPhases currentPhase;
  private final Map<String, String> hostIpAddresses = new ConcurrentHashMap<>();
  private final Map<String, String> correspondingNodes = new ConcurrentHashMap<>();

  public BlueGreenStatus(
      final BlueGreenPhases phase,
      final Map<String, String> hostIpAddresses,
      final Map<String, String> correspondingNodes) {

    this.currentPhase = phase;
    this.hostIpAddresses.putAll(hostIpAddresses);
    this.correspondingNodes.putAll(correspondingNodes);
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
}
