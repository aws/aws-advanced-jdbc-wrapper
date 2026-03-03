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

package software.amazon.jdbc.plugin.efm.v2;

import java.sql.Connection;
import software.amazon.jdbc.plugin.efm.base.HostMonitorConnectionContext;

/**
 * Monitoring context for each connection. This contains each connection's criteria for whether a
 * server should be considered unhealthy. The context is shared between the main thread and the monitor thread.
 */
public class HostMonitorConnectionContextV2 extends HostMonitorConnectionContext {

  /**
   * Constructor.
   *
   * @param connectionToAbort A reference to the connection associated with this context that will be aborted.
   */
  public HostMonitorConnectionContextV2(final Connection connectionToAbort) {
    super(connectionToAbort);
  }
}
