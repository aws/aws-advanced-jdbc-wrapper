/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.plugin.failover;

import java.sql.SQLException;
import java.util.List;
import software.aws.rds.jdbc.proxydriver.HostSpec;

/**
 * Interface for Writer Failover Process handler. This handler implements all necessary logic to try
 * to reconnect to a current writer host or to a newly elected writer.
 */
public interface WriterFailoverHandler {

  /**
   * Called to start Writer Failover Process.
   *
   * @param currentTopology Cluster current topology
   * @return {@link WriterFailoverResult} The results of this process.
   */
  WriterFailoverResult failover(List<HostSpec> currentTopology) throws SQLException;
}
