/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.plugin.efm;

import java.sql.Connection;
import java.util.Properties;
import java.util.Set;
import software.aws.rds.jdbc.proxydriver.HostSpec;

/**
 * Interface for monitor services. This class implements ways to start and stop monitoring servers
 * when connections are created.
 */
public interface MonitorService {

  MonitorConnectionContext startMonitoring(
      Connection connectionToAbort,
      Set<String> nodeKeys,
      HostSpec hostSpec,
      Properties properties,
      int failureDetectionTimeMillis,
      int failureDetectionIntervalMillis,
      int failureDetectionCount);

  /**
   * Stop monitoring for a connection represented by the given {@link MonitorConnectionContext}.
   * Removes the context from the {@link MonitorImpl}.
   *
   * @param context The {@link MonitorConnectionContext} representing a connection.
   */
  void stopMonitoring(MonitorConnectionContext context);

  /**
   * Stop monitoring the node for all connections represented by the given set of node keys.
   *
   * @param nodeKeys All known references to a server.
   */
  void stopMonitoringForAllConnections(Set<String> nodeKeys);

  void releaseResources();

  /**
   * Handle unused {@link Monitor}.
   *
   * @param monitor The {@link Monitor} in idle.
   */
  void notifyUnused(Monitor monitor);
}
