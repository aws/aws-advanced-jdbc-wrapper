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

package software.amazon.jdbc.plugin.efm;

import java.sql.Connection;
import java.util.Properties;
import java.util.Set;
import software.amazon.jdbc.HostSpec;

/**
 * Interface for monitor services. This class implements ways to start and stop monitoring servers
 * when connections are created.
 */
public interface HostMonitorService {

  HostMonitorConnectionContext startMonitoring(
      Connection connectionToAbort,
      Set<String> nodeKeys,
      HostSpec hostSpec,
      Properties properties,
      int failureDetectionTimeMillis,
      int failureDetectionIntervalMillis,
      int failureDetectionCount);

  /**
   * Stop monitoring for a connection represented by the given {@link HostMonitorConnectionContext}.
   * Removes the context from the {@link HostMonitorImpl}.
   *
   * @param context The {@link HostMonitorConnectionContext} representing a connection.
   */
  void stopMonitoring(HostMonitorConnectionContext context);

  /**
   * Stop monitoring the node for all connections represented by the given set of node keys.
   *
   * @param nodeKeys All known references to a server.
   */
  void stopMonitoringForAllConnections(Set<String> nodeKeys);

  void releaseResources();
}
