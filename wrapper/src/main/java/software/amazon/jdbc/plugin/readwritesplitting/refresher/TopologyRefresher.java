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

package software.amazon.jdbc.plugin.readwritesplitting.refresher;

import java.sql.SQLException;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;

/**
 * Refreshes and stores cluster topology before a routing decision ("how are hosts discovered?").
 * The topology implementation refreshes the host list and records the writer host; the endpoint
 * (Simple) implementation is a no-op because it does not rely on topology.
 */
public interface TopologyRefresher {

  void refresh(RwSplitContext ctx) throws SQLException;
}
