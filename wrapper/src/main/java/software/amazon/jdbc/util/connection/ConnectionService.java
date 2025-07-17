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

package software.amazon.jdbc.util.connection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;

public interface ConnectionService {
  /**
   * Creates an auxiliary connection. Auxiliary connections are driver-internal connections that accomplish various
   * specific tasks such as monitoring a host's availability, checking the topology information for a cluster, etc.
   *
   * @param hostSpec the hostSpec containing the host information for the auxiliary connection.
   * @param props    the properties for the auxiliary connection.
   * @return a new connection to the given host using the given props.
   * @throws SQLException if an error occurs while opening the connection.
   */
  Connection open(HostSpec hostSpec, Properties props) throws SQLException;

  PluginService getPluginService();
}
