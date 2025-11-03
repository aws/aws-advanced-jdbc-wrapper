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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import software.amazon.jdbc.HostSpec;

public class GlobalAuroraDialectUtils extends AuroraDialectUtils {
  public GlobalAuroraDialectUtils(String writerIdQuery) {
    super(writerIdQuery);
  }

  @Override
  protected TopologyQueryHostSpec createHost(final ResultSet resultSet) throws SQLException {
    // The topology query results should contain 4 columns:
    // node ID, 1/0 (writer/reader), node lag in time (ms), AWS region.
    String hostName = resultSet.getString(1);
    final boolean isWriter = resultSet.getBoolean(2);
    final float nodeLag = resultSet.getFloat(3);
    final String region = resultSet.getString(4);

    // Calculate weight based on node lag in time and CPU utilization.
    final long weight = Math.round(nodeLag) * 100L;
    return new TopologyQueryHostSpec(hostName, isWriter, weight, Timestamp.from(Instant.now()), region);
  }
}
