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

package software.amazon.jdbc.plugin.limitless;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;

public interface LimitlessRouterService {

  List<HostSpec> getLimitlessRouters(final String clusterId, final Properties props) throws SQLException;

  List<HostSpec> forceGetLimitlessRouters(
      final Connection connection, final JdbcCallable<Connection, SQLException> connectFunc, final int hostPort,
      final Properties props)  throws SQLException;

  void startMonitoring(
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final int intervalMs);
}
