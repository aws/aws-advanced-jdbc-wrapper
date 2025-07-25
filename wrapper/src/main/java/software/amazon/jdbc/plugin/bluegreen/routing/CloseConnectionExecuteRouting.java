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

package software.amazon.jdbc.plugin.bluegreen.routing;

import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenRole;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.storage.StorageService;

// Close current connection.
public class CloseConnectionExecuteRouting extends BaseExecuteRouting {

  private static final Logger LOGGER = Logger.getLogger(CloseConnectionExecuteRouting.class.getName());

  public CloseConnectionExecuteRouting(@Nullable String hostAndPort, @Nullable BlueGreenRole role) {
    super(hostAndPort, role);
  }

  @Override
  public <T, E extends Exception> Optional<T> apply(
      final ConnectionPlugin plugin,
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs,
      final StorageService storageService,
      final PluginService pluginService,
      final Properties props) throws E {

    try {
      if (pluginService.getCurrentConnection() != null
          && !pluginService.getCurrentConnection().isClosed()) {
        pluginService.getCurrentConnection().close();
      }

      throw new SQLException(Messages.get("bgd.inProgressConnectionClosed"), "08001");

    } catch (SQLException ex) {
      throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, ex);
    }
  }
}
