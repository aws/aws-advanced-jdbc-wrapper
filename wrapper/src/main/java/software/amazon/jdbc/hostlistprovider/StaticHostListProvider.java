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

package software.amazon.jdbc.hostlistprovider;

import java.sql.SQLException;
import software.amazon.jdbc.HostRole;

// A marker interface for providers that fetch host lists that do not change over time.
// An example is a provider that uses a connection string to determine the host list.
public interface StaticHostListProvider extends HostListProvider {

  /**
   * Records the measured role of a host in this provider's host list.
   *
   * <p>A static host list has no topology to consult, so it derives roles from the connection
   * string alone. When a plugin opens a connection and measures the host's actual role, this method
   * lets that measurement replace the assumed role, so that reader/writer selection operates on the
   * measured value rather than on the ordering of the connection string.
   *
   * <p>The default implementation does nothing, so a provider that cannot revise its roles is
   * unaffected.
   *
   * @param hostAndPort the host to update, in {@code host:port} form as returned by
   *                    {@link software.amazon.jdbc.HostSpec#getHostAndPort()}
   * @param role        the measured role of that host
   * @return true if a host list entry was changed as a result of this call, false if the host is not
   *     in the list, already carries this role, or this provider does not support role updates
   * @throws SQLException if the host list could not be read
   */
  default boolean updateHostRole(final String hostAndPort, final HostRole role) throws SQLException {
    return false;
  }
}
