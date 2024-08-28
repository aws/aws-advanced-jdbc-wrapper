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

import java.util.Properties;

@FunctionalInterface
public interface AcceptsUrlFunc {

  /**
   * This function can be passed to a {@link ConnectionProvider} constructor to specify when the
   * {@link ConnectionProvider} should be used to open a connection to the given {@link HostSpec} with the
   * given {@link Properties}.
   *
   * @param hostSpec      the host details for the requested connection
   * @param props         the properties for the requested connection
   * @return a boolean indicating whether a {@link ConnectionProvider} should be used to open a connection to the given
   *     {@link HostSpec} with the given {@link Properties}.
   */
  boolean acceptsUrl(HostSpec hostSpec, Properties props);
}
