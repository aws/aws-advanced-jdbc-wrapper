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

import java.util.Optional;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenRole;
import software.amazon.jdbc.util.storage.StorageService;

public abstract class BaseExecuteRouting extends BaseRouting implements ExecuteRouting {

  protected final String hostAndPort;
  protected BlueGreenRole role;

  public BaseExecuteRouting(@Nullable String hostAndPort, @Nullable BlueGreenRole role) {
    this.hostAndPort = hostAndPort == null ? null : hostAndPort.toLowerCase();
    this.role = role;
  }

  @Override
  public boolean isMatch(HostSpec hostSpec, BlueGreenRole hostRole) {
    return (this.hostAndPort == null || this.hostAndPort.equals(
              hostSpec == null ? null : hostSpec.getHostAndPort().toLowerCase()))
        && (this.role == null || this.role.equals(hostRole));
  }

  @Override
  public abstract <T, E extends Exception> @NonNull Optional<T> apply(
      final ConnectionPlugin plugin,
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs,
      final StorageService storageService,
      final PluginService pluginService,
      final Properties props) throws E;

  @Override
  public String toString() {
    return String.format("%s [%s, %s]",
        super.toString(),
        this.hostAndPort == null ? "<null>" : this.hostAndPort,
        this.role == null ? "<null>" : this.role.toString());
  }
}
