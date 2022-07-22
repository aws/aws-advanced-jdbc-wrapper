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

package com.amazon.awslabs.jdbc.plugin.efm;

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
