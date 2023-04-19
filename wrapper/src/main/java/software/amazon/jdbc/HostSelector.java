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

package software.amazon.jdbc;

import java.sql.SQLException;
import java.util.List;

public interface HostSelector {

  /**
   * Selects a host with the requested role from the given host list.
   *
   * @param hosts a list of available hosts to pick from
   * @param role  the desired host role - either a writer or a reader
   * @return a host matching the requested role
   * @throws SQLException if the host list does not contain any hosts matching the requested role or
   *                      an error occurs while selecting a host
   */
  HostSpec getHost(List<HostSpec> hosts, HostRole role) throws SQLException;
}
