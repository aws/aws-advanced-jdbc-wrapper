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

package integration.container.tests.metrics;

import integration.container.tests.metrics.DatabasePerformanceMetricTest.ClusterInstanceInfo;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TopologyEventHolder {
  public final String nodeId;
  public final Instant timestamp;
  public final long timestampNano;
  public final String writerHostId;
  public final Set<String> readerHostIds;
  public final boolean accessible;
  public final Boolean readOnly;
  public final boolean blankTopology;

  private long offsetTimeMs;

  public TopologyEventHolder(
      String nodeId,
      Instant timestamp,
      long timestampNano,
      String writerHostId,
      Set<String> readerHostIds,
      boolean accessible,
      boolean readOnly,
      boolean blankTopology) {
    this.nodeId = nodeId;
    this.timestamp = timestamp;
    this.timestampNano = timestampNano;
    this.writerHostId = writerHostId;
    this.readerHostIds = readerHostIds;
    this.accessible = accessible;
    this.readOnly = readOnly;
    this.blankTopology = blankTopology;
  }

  public TopologyEventHolder(
      String nodeId,
      Instant timestamp,
      long timestampNano,
      boolean accessible,
      List<ClusterInstanceInfo> topology,
      Boolean readOnly) {
    this.nodeId = nodeId;
    this.timestamp = timestamp;
    this.timestampNano = timestampNano;
    this.accessible = accessible;
    this.readOnly = readOnly;
    this.blankTopology = topology == null || topology.isEmpty();
    this.writerHostId = topology == null || topology.isEmpty()
        ? null
        : topology.stream().filter(x -> x.isWriter).map(x -> x.hostId).findFirst().orElse(null);
    this.readerHostIds = topology == null || topology.isEmpty()
        ? null
        : topology.stream().filter(x -> !x.isWriter).map(x -> x.hostId).collect(Collectors.toSet());
  }

  public void setOffsetTimeMs(long offsetTimeMs) {
    this.offsetTimeMs = offsetTimeMs;
  }

  public long getOffsetTimeMs() {
    return offsetTimeMs;
  }

  @Override
  public int hashCode() {
    int result = 17;
    if (this.nodeId != null) {
      result = 31 * result + this.nodeId.hashCode();
    }
    result = 31 * result + Boolean.hashCode(this.accessible);
    if (this.writerHostId != null) {
      result = 31 * result + this.writerHostId.hashCode();
    }
    if (this.readerHostIds != null) {
      result = 31 * result + this.readerHostIds.hashCode();
    }
    if (this.readOnly != null) {
      result = 31 * result + this.readOnly.hashCode();
    }
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TopologyEventHolder)) {
      return false;
    }
    return this.hashCode() == obj.hashCode();
  }

  @Override
  public String toString() {
    return super.toString()
        + String.format(" [nodeId=%s, timestamp=%s, time=%d, accessible=%s, readOnly=%s, writer=%s, readers=%s]",
        this.nodeId == null ? "<null>" : this.nodeId,
        this.timestamp == null ? "<null>" : this.timestamp.toString(),
        this.timestampNano,
        this.accessible,
        this.readOnly == null ? "<null>" : this.readOnly.toString(),
        this.writerHostId == null ? "<null>" : this.writerHostId,
        this.readerHostIds == null
            ? "<null>"
            : "[" + this.readerHostIds.stream().sorted().collect(Collectors.joining(", ")) + "]");
  }
}
