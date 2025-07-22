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

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

public class RunData implements Serializable {

  private static final long serialVersionUID = 1L;

  public Instant startTime;
  public List<RunDataRow> topologyRows;
  public String topologyError;

  public List<FailoverResult> failoverResults;

  // Add more analyzed metrics below
  public boolean topologyFalsePositive; // old writer node gets available and reports a stale writer (node "1")
  public boolean topologyOldWriterUnavailable; // old writer node is unavailable after failover
  public boolean failoverFail;

  public RunData() {
  }
}
