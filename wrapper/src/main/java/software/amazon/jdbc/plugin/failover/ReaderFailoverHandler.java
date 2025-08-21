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

package software.amazon.jdbc.plugin.failover;

import java.sql.SQLException;
import java.util.List;
import software.amazon.jdbc.HostSpec;

/**
 * Interface for Reader Failover Process handler. This handler implements all necessary logic to try
 * to reconnect to another reader host.
 */
public interface ReaderFailoverHandler {

  /**
   * Called to start Reader Failover Process. This process tries to connect to any reader. If no
   * reader is available then driver may also try to connect to a writer host, down hosts, and the
   * current reader host.
   *
   * @param hosts Cluster current topology.
   * @param currentHost The currently connected host that has failed.
   * @return {@link ReaderFailoverResult} The results of this process.
   * @throws SQLException indicating whether the failover attempt was successful.
   */
  ReaderFailoverResult failover(List<HostSpec> hosts, HostSpec currentHost) throws SQLException;

  /**
   * Called to get any available reader connection. If no reader is available then result of process
   * is unsuccessful. This process will not attempt to connect to the writer host.
   *
   * @param hostList Cluster current topology.
   * @return {@link ReaderFailoverResult} The results of this process.
   * @throws SQLException if any error occurred while attempting a reader connection.
   */
  ReaderFailoverResult getReaderConnection(List<HostSpec> hostList) throws SQLException;
}
