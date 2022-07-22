/*
 *
 *     Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License").
 *     You may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.amazon.awslabs.jdbc.plugin.failover;

import com.amazon.awslabs.jdbc.HostSpec;
import java.sql.SQLException;
import java.util.List;

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
