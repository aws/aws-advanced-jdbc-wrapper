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

package software.amazon.jdbc.hostlistprovider;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import software.amazon.jdbc.HostSpec;

// An interface for providers that can fetch a host list reflecting the current database topology.
// Examples include providers for Aurora or Multi-AZ clusters, where the cluster topology, status, and instance roles
// change over time.
public interface DynamicHostListProvider extends HostListProvider {
  List<HostSpec> queryForTopology(Connection conn, HostSpec initialHostSpec) throws SQLException;
}
