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

package software.amazon.jdbc.plugin.strategy.fastestresponse;

import java.util.List;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;

public interface HostResponseTimeService {

  /**
   * Return a response time in milliseconds to the host.
   * Return Integer.MAX_VALUE if response time is not available.
   *
   * @param hostSpec the host details
   * @return response time in milliseconds for a desired host. It should return Integer.MAX_VALUE if
   *            response time couldn't be measured.
   */
  int getResponseTime(final HostSpec hostSpec);

  /**
   * Provides an updated host list to a service.
   */
  void setHosts(final @NonNull List<HostSpec> hosts);
}
