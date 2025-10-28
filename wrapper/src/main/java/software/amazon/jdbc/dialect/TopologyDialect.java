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
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;

public interface TopologyDialect {
  String getTopologyQuery();

  @Nullable
  List<TopologyQueryHostSpec> processQueryResults(ResultSet rs, @Nullable String suggestedWriterId)
      throws SQLException;

  @Nullable
  String getWriterId(final Connection connection) throws SQLException;

  // TODO: can we remove this and use getHostRole instead?
  boolean isWriterInstance(final Connection connection) throws SQLException;

  HostSpec identifyConnection(Connection connection) throws SQLException

  String getIsReaderQuery();

  String get
}
