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
public interface HikariPoolMapping {

  /**
   * A function that can optionally be passed to the {@link HikariPooledConnectionProvider}
   * constructor to define a mapping from the given parameters to an internal connection pool key.
   *
   * <p>By default, the HikariPooledConnectionProvider will create an internal connection pool for
   * each
   * database instance in the database cluster. This function allows you to define your own key
   * mapping if the default behavior does not suit your needs. For example, if your application
   * establishes multiple connections to the same instance under different users, you can implement
   * this function to return a key that incorporates the {@link HostSpec} URL and the user specified
   * in the properties. The resulting HikariPooledConnectionProvider will then create a new internal
   * connection pool for each instance-user combination.
   *
   * @param hostSpec      the host details for the internal connection pool
   * @param originalProps the properties specified for the original connection through
   *                      {@link java.sql.DriverManager#getConnection(String, Properties)}
   * @return the key that should be used for the given host and properties. An internal connection
   *     pool will be created for each unique key returned by this function.
   */
  String getKey(HostSpec hostSpec, Properties originalProps);
}
