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

package software.amazon.jdbc.benchmarks.support;

import java.sql.Connection;
import java.util.List;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionInfo;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.Pair;

/**
 * A {@link ConnectionProvider} that hands out a pre-built {@link FakeJdbc} connection instead of
 * opening a socket.
 *
 * <p>This is what lets the wrapper's real {@code connect} pipeline - plugin chain,
 * {@link ConnectionProviderManager} dispatch, dialect handling - be benchmarked end to end with no
 * database. Connection establishment itself is a constant here, so differences between benchmarks
 * are attributable to the wrapper.
 */
public class FakeConnectionProvider implements ConnectionProvider {

  private final Connection connection;
  private final @Nullable HostSpec hostByStrategy;

  public FakeConnectionProvider(final Connection connection) {
    this(connection, null);
  }

  public FakeConnectionProvider(final Connection connection, final @Nullable HostSpec hostByStrategy) {
    this.connection = connection;
    this.hostByStrategy = hostByStrategy;
  }

  @Override
  public boolean acceptsUrl(
      final @NonNull String protocol, final @NonNull HostSpec hostSpec, final @NonNull Properties props) {
    return true;
  }

  @Override
  public boolean acceptsStrategy(final @Nullable HostRole role, final @NonNull String strategy) {
    return this.hostByStrategy != null;
  }

  @Override
  public @Nullable HostSpec getHostSpecByStrategy(
      final @NonNull List<HostSpec> hosts,
      final @Nullable HostRole role,
      final @NonNull String strategy,
      final @Nullable Properties props) {
    return this.hostByStrategy;
  }

  @Override
  public @NonNull ConnectionInfo connect(
      final @NonNull String protocol,
      final @NonNull Dialect dialect,
      final @NonNull TargetDriverDialect targetDriverDialect,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) {
    return new ConnectionInfo(this.connection, false);
  }

  @Override
  public String getTargetName() {
    return "fake";
  }

  @Override
  public @Nullable List<Pair<String, Object>> getSnapshotState() {
    return null;
  }
}
