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
 * Interface for Reader Failover Process handler. This handler implements all necessary logic to try to reconnect to
 * another reader host.
 */
public interface ReaderFailoverHandler {

  /**
   * Called to start Reader Failover Process. This process tries to connect to any reader. If no reader is available
   * then driver may also try to connect to a writer host, down hosts, and the current reader host.
   *
   * @param hosts       Cluster current topology.
   * @param currentHost The currently connected host that has failed.
   * @return {@link ReaderFailoverResult} The results of this process.
   */
  ReaderFailoverResult failover(List<HostSpec> hosts, HostSpec currentHost) throws SQLException;

  /**
   * Called to get any available reader connection. If no reader is available then result of process is unsuccessful.
   * This process will not attempt to connect to the writer host.
   *
   * @param hostList Cluster current topology.
   * @return {@link ReaderFailoverResult} The results of this process.
   */
  ReaderFailoverResult getReaderConnection(List<HostSpec> hostList) throws SQLException;
}
