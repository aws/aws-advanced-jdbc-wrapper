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

package software.amazon.jdbc.util;

import java.sql.Connection;
import java.sql.SQLException;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;

/**
 * Caches the result of {@code dialect.getHostId()} keyed by the connection host name.
 * The cached value is a {@link Pair} of (hostId, hostName) as returned by the dialect.
 */
public interface HostIdCacheService {

  /**
   * Identify connected host using cache.
   *
   * @param connection connection to be identified
   * @param connectionHostSpec HostSpec of provided connection
   * @param pluginService pluginService instance
   * @return identified HostSpec for a connection
   * @throws SQLException if an error occurs while identifying the host
   */
  @Nullable HostSpec identifyConnection(
      final @NonNull Connection connection,
      final @NonNull HostSpec connectionHostSpec,
      final @NonNull PluginService pluginService) throws SQLException;
}
