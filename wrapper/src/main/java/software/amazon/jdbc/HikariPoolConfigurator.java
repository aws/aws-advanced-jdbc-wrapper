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

import com.zaxxer.hikari.HikariConfig;
import java.util.Properties;

@FunctionalInterface
public interface HikariPoolConfigurator {

  /**
   * A function that should be passed to the {@link HikariPooledConnectionProvider} constructor to
   * specify any extra configuration options for an internal Hikari connection pool to the given
   * {@link HostSpec}. By default, the HikariPooledConnectionProvider will set the jdbcUrl,
   * exceptionOverrideClassName, username, and password of the {@link HikariConfig} returned by this
   * method. If no extra configuration options are required, this method should simply return an
   * empty HikariConfig.
   *
   * @param hostSpec      the details of the host for the internal connection pool
   * @param originalProps the properties specified for the original connection through
   *                      {@link java.sql.DriverManager#getConnection(String, Properties)}
   * @return a {@link HikariConfig} specifying any extra configuration options for an internal
   *     Hikari connection pool maintained by a {@link HikariPooledConnectionProvider}
   */
  HikariConfig configurePool(HostSpec hostSpec, Properties originalProps);
}
