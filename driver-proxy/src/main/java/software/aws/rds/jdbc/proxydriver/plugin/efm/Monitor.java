/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.plugin.efm;

/**
 * Interface for monitors. This class uses background threads to monitor servers with one or more
 * connections for more efficient failure detection during method execution.
 */
public interface Monitor extends Runnable {

  void startMonitoring(MonitorConnectionContext context);

  void stopMonitoring(MonitorConnectionContext context);

  /** Clear all {@link MonitorConnectionContext} associated with this {@link Monitor} instance. */
  void clearContexts();

  /**
   * Whether this {@link Monitor} has stopped monitoring a particular server.
   *
   * @return true if the monitoring has stopped; false otherwise.
   */
  boolean isStopped();
}
