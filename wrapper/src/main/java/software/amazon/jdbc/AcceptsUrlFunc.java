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
   * A function that can optionally be passed to the {@link HikariPooledConnectionProvider}
   * to define when an internal connection pool should be created for a connection with the given {@link HostSpec} and
   * {@link Properties}.
   *
   * <p>By default, the HikariPooledConnectionProvider will create an internal connection pool if the passed in
   * {@link HostSpec} specifies a URL with a standard RDS instance format. If you would like to change this default
   * behaviour, you can pass in a function with this interface to define your own behaviour. If the connection provider
   * has been set to a {@link HikariPooledConnectionProvider} via
   * {@link ConnectionProviderManager#setConnectionProvider(ConnectionProvider)}, this function will be called when the
   * connect pipeline is used. If the function returns true, an internal connection pool will be created for the
   * connection to the host specified by the given {@link HostSpec}.
   *
   * @param hostSpec      the host details for the requested connection
   * @param props         the properties specified for the requested connection
   * @return a boolean indicating whether an internal connection pool should be created for a connection specified by
   *     the given {@link HostSpec} and {@link Properties}.
   */
  boolean acceptsUrl(HostSpec hostSpec, Properties props);
}
