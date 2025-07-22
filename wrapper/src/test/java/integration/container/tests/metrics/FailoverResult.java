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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.Serializable;
import java.time.Instant;
import software.amazon.jdbc.plugin.failover.FailoverMode;

public class FailoverResult implements Serializable {
  private static final long serialVersionUID = 1L;

  @JsonIgnore
  public final String nodeId;
  public final FailoverMode failoverMode;
  public final Instant timestamp;
  @JsonIgnore
  public final long timestampNano;
  public final boolean success;
  @JsonIgnore
  public final String connectedHostId;
  public final String error;

  private long offsetTimeMs;
  public int mappedHostId;
  public int mappedConnectedHostId;

  public FailoverResult(
      String nodeId,
      FailoverMode failoverMode,
      Instant timestamp,
      long timestampNano,
      boolean success,
      String connectedHostId,
      String error) {
    this.nodeId = nodeId;
    this.failoverMode = failoverMode;
    this.timestamp = timestamp;
    this.timestampNano = timestampNano;
    this.success = success;
    this.connectedHostId = connectedHostId;
    this.error = error;
  }

  public long getOffsetTimeMs() {
    return offsetTimeMs;
  }

  public void setOffsetTimeMs(long offsetTimeMs) {
    this.offsetTimeMs = offsetTimeMs;
  }

  public int getMappedConnectedHostId() {
    return mappedConnectedHostId;
  }

  public void setMappedConnectedHostId(int mappedConnectedHostId) {
    this.mappedConnectedHostId = mappedConnectedHostId;
  }

  public int getMappedHostId() {
    return mappedHostId;
  }

  public void setMappedHostId(int mappedHostId) {
    this.mappedHostId = mappedHostId;
  }

  @Override
  public int hashCode() {
    int result = 17;
    if (this.nodeId != null) {
      result = 31 * result + this.nodeId.hashCode();
    }
    if (this.failoverMode != null) {
      result = 31 * result + this.failoverMode.hashCode();
    }
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FailoverResult)) {
      return false;
    }
    return this.hashCode() == obj.hashCode();
  }

  @Override
  public String toString() {
    return super.toString()
        + String.format(
            " [nodeId=%s, failoverMode=%s, timestamp=%s, time=%d, offsetTime=%d, success=%s,"
            + " connectedHost=%s, mappedConnectedHost=%d]",
            this.nodeId == null ? "<null>" : this.nodeId,
            this.failoverMode == null ? "<null>" : this.failoverMode.toString(),
            this.timestamp == null ? "<null>" : this.timestamp.toString(),
            this.timestampNano,
            this.offsetTimeMs,
            this.success,
            this.connectedHostId == null ? "<null>" : this.connectedHostId,
            this.mappedConnectedHostId);
  }
}
