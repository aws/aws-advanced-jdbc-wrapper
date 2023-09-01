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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import software.amazon.jdbc.hostlistprovider.RdsMultiAzDbClusterListProvider;

public class RdsMultiAzDbClusterMysqlDialect extends MysqlDialect {

  private static final String TOPOLOGY_QUERY =
      "SELECT id, endpoint, port FROM mysql.rds_topology";

  private static final String FETCH_WRITER_NODE_QUERY =
      "SHOW REPLICA STATUS";

  private static final String FETCH_WRITER_NODE_QUERY_HEADER = "Source_Server_Id";

  private static final String NODE_ID_QUERY = "SELECT @@server_id";
  private static final String IS_READER_QUERY = "SELECT @@read_only";

  @Override
  public boolean isDialect(final Connection connection) {
    try (final Statement stmt = connection.createStatement();
         final ResultSet rs = stmt.executeQuery("SELECT id FROM mysql.rds_topology LIMIT 1")) {
      if (rs.next()) {
        // If the table is presented it means it's a Multi-AZ DB cluster
        return true;
      }
    } catch (final SQLException ex) {
      // ignore
    }
    return false;
  }

  @Override
  public List</* dialect code */ String> getDialectUpdateCandidates() {
    return null;
  }

  @Override
  public HostListProviderSupplier getHostListProvider() {
    return (properties, initialUrl, hostListProviderService) -> new RdsMultiAzDbClusterListProvider(
        properties,
        initialUrl,
        hostListProviderService,
        TOPOLOGY_QUERY,
        NODE_ID_QUERY,
        IS_READER_QUERY,
        FETCH_WRITER_NODE_QUERY,
        FETCH_WRITER_NODE_QUERY_HEADER);
  }
}
