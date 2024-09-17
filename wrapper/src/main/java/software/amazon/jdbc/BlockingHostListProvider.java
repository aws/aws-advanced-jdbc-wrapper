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
import java.util.concurrent.TimeoutException;

public interface BlockingHostListProvider extends HostListProvider {

  /**
   * Force a host list provider to update its topology information. Results will be returned when the topology is
   * updated or the writer is verified, unless the timeout is hit.
   *
   * @param shouldVerifyWriter a flag indicating that the provider should verify the writer before
   *                           returning the updated topology.
   * @param timeoutMs timeout in msec to wait until topology is updated or the writer is verified.
   *                  If a timeout of 0 is provided, a topology update will be initiated but cached topology
   *                  will be returned. If a non-zero timeout is provided and the timeout is hit,
   *                  a TimeoutException will be thrown.
   * @return a list of host details representing a cluster topology
   */
  List<HostSpec> forceRefresh(final boolean shouldVerifyWriter, final long timeoutMs)
      throws SQLException, TimeoutException;
}
