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

package software.amazon.jdbc.plugin.readwritesplitting;

import java.util.ArrayList;
import java.util.List;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.util.Pair;

/** Shared {@link SnapshotContributor} fragments reused across read/write splitting assemblies. */
public final class RwSplitSnapshots {

  private RwSplitSnapshots() {
  }

  /** Fragment exposing the reader host selector strategy (topology-based codes). */
  public static SnapshotContributor readerStrategy(final String strategy) {
    return () -> {
      final List<Pair<String, Object>> state = new ArrayList<>();
      state.add(Pair.create("readerSelectorStrategy", strategy));
      return state;
    };
  }

  /** Fragment exposing the endpoint (Simple) configuration. */
  public static SnapshotContributor endpoint(
      final boolean verifyNewConnections,
      final String writeEndpoint,
      final String readEndpoint,
      final HostRole verifyOpenedConnectionType,
      final int retryIntervalMs,
      final long retryTimeoutMs) {
    return () -> {
      final List<Pair<String, Object>> state = new ArrayList<>();
      state.add(Pair.create("verifyNewConnections", verifyNewConnections));
      state.add(Pair.create("writeEndpoint", writeEndpoint));
      state.add(Pair.create("readEndpoint", readEndpoint));
      state.add(Pair.create("verifyOpenedConnectionType",
          verifyOpenedConnectionType != null ? verifyOpenedConnectionType.toString() : null));
      state.add(Pair.create("connectRetryIntervalMs", retryIntervalMs));
      state.add(Pair.create("connectRetryTimeoutMs", retryTimeoutMs));
      return state;
    };
  }
}
