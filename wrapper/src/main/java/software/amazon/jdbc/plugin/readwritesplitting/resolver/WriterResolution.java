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

package software.amazon.jdbc.plugin.readwritesplitting.resolver;

import java.sql.Connection;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;

/**
 * The outcome of a {@link WriterResolver#resolveWriter} call.
 *
 * <ul>
 *   <li>{@link Kind#CONNECTED} — a writer connection was established; {@link #getConnection()} and
 *       {@link #getHostSpec()} are non-null.</li>
 *   <li>{@link Kind#STAY} — a writer is "needed" but the plugin should remain on the current
 *       (reader) connection. Used for Global Write Forwarding, where writes are forwarded from a
 *       reader in a secondary region and no separate writer connection is opened.</li>
 * </ul>
 */
public final class WriterResolution {

  public enum Kind {
    CONNECTED,
    STAY
  }

  private static final WriterResolution STAY_RESOLUTION = new WriterResolution(Kind.STAY, null, null);

  private final Kind kind;
  private final @Nullable Connection connection;
  private final @Nullable HostSpec hostSpec;

  private WriterResolution(
      final Kind kind, final @Nullable Connection connection, final @Nullable HostSpec hostSpec) {
    this.kind = kind;
    this.connection = connection;
    this.hostSpec = hostSpec;
  }

  public static WriterResolution connected(final Connection connection, final HostSpec hostSpec) {
    return new WriterResolution(Kind.CONNECTED, connection, hostSpec);
  }

  public static WriterResolution stay() {
    return STAY_RESOLUTION;
  }

  public Kind getKind() {
    return this.kind;
  }

  public boolean isConnected() {
    return this.kind == Kind.CONNECTED;
  }

  public @Nullable Connection getConnection() {
    return this.connection;
  }

  public @Nullable HostSpec getHostSpec() {
    return this.hostSpec;
  }
}
