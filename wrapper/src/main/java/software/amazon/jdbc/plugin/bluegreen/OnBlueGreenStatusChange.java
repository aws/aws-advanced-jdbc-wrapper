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

package software.amazon.jdbc.plugin.bluegreen;

@FunctionalInterface
public interface OnBlueGreenStatusChange {

  /**
   * Reports a monitor's latest view of the deployment.
   *
   * @param monitor      the monitor reporting the status. The receiver uses this to recognize a
   *                     status produced by a monitor it has already discarded.
   * @param role         the role the monitor is monitoring.
   * @param interimStatus the status the monitor collected.
   */
  void onBlueGreenStatusChanged(
      BlueGreenStatusMonitor monitor, BlueGreenRole role, BlueGreenInterimStatus interimStatus);
}
