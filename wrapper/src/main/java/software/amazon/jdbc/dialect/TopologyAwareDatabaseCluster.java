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

public interface TopologyAwareDatabaseCluster {

  /**
   * Query should return multiple rows with the following columns. Column order is important!
   * 1 - A string that represents a node name or node ID
   * 2 - A boolean that represents a node role (true - writer, false - reader/replica)
   * 3 - A number (or null) that represents a current CPU utilization of a node. Lesser number
   * corresponds to less node load/utilization
   * 4 - A number (or null) that represent a node lag comparing to a writer node. Lesser number
   * corresponds to small lag in time.
   */
  String getTopologyQuery();

  /**
   * Query should return a single row with a single column.
   */
  String getNodeIdQuery();

  /**
   * Query should return a single row with a single boolean column.
   */
  String getIsReaderQuery();
}
