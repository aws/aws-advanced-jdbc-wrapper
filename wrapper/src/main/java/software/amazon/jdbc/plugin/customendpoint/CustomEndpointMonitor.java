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

package software.amazon.jdbc.plugin.customendpoint;

/**
 * Interface for custom endpoint monitors. Custom endpoint monitors analyze a given custom endpoint for custom endpoint
 * information and future changes to the endpoint.
 */
public interface CustomEndpointMonitor extends AutoCloseable, Runnable {

  /**
   * Evaluates whether the monitor should be disposed.
   *
   * @return true if the monitor should be disposed, otherwise return false.
   */
  boolean shouldDispose();

  /**
   * Indicates whether the monitor has info about the custom endpoint or not. This will be false if the monitor is new
   * and has not yet had enough time to fetch the info.
   *
   * @return true if the monitor has info about the custom endpoint, otherwise returns false.
   */
  boolean hasCustomEndpointInfo();
}
