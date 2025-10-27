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

package software.amazon.jdbc.dialect;

import java.sql.Timestamp;

public class TopologyQueryHostSpec {
  private final String instanceId;
  private final boolean isWriter;
  private final long weight;
  private final Timestamp lastUpdateTime;

  public TopologyQueryHostSpec(String instanceId, boolean isWriter, long weight, Timestamp lastUpdateTime) {
    this.instanceId = instanceId;
    this.isWriter = isWriter;
    this.weight = weight;
    this.lastUpdateTime = lastUpdateTime;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public boolean isWriter() {
    return isWriter;
  }

  public long getWeight() {
    return weight;
  }

  public Timestamp getLastUpdateTime() {
    return lastUpdateTime;
  }
}
