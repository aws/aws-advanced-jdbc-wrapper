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

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Interface for connection plugins. This class implements ways to execute a JDBC method and to
 * clean up resources used before closing the plugin.
 */
public interface ConnectionPlugin {

  Set<String> getSubscribedMethods();

  <T, E extends Exception> T execute(
      final @Nullable Class<T> resultClass,
      final @NonNull Class<E> exceptionClass,
      final @NonNull Object methodInvokeOn,
      final @NonNull String methodName,
      final @NonNull JdbcCallable<T, E> jdbcMethodFunc,
      final @Nullable Object @NonNull[] jdbcMethodArgs)
      throws E;

  Connection connect(
      final @NonNull String driverProtocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final boolean isInitialConnection,
      final @NonNull JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException;

  void initHostProvider(
      final @NonNull String driverProtocol,
      final @NonNull String initialUrl,
      final @NonNull Properties props,
      final @NonNull HostListProviderService hostListProviderService,
      final @Nullable JdbcCallable<Void, SQLException> initHostProviderFunc)
      throws SQLException;

  OldConnectionSuggestedAction notifyConnectionChanged(EnumSet<NodeChangeOptions> changes);

  void notifyNodeListChanged(Map<String, EnumSet<NodeChangeOptions>> changes);
}
