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
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.DialectProvider;

/**
 * A {@link DialectProvider} that always returns the same dialect and reports it as confirmed.
 *
 * <p>Lets {@code PluginServiceImpl} be constructed in a benchmark. The default
 * {@code DialectManager} performs endpoint- and query-based dialect detection during construction,
 * which needs a real database; supplying a fixed dialect skips detection without changing any of the
 * per-call paths being measured afterwards.
 */
public class FixedDialectProvider implements DialectProvider {

  private final Dialect dialect;

  public FixedDialectProvider(final Dialect dialect) {
    this.dialect = dialect;
  }

  @Override
  public Dialect getDialect(
      final @NonNull String driverProtocol, final @NonNull String url, final @NonNull Properties props) {
    return this.dialect;
  }

  @Override
  public Dialect getDialect(
      final @NonNull String originalUrl,
      final @NonNull HostSpec hostSpec,
      final @NonNull Connection connection) {
    return this.dialect;
  }

  @Override
  public boolean isConfirmedDialect() {
    return true;
  }
}
