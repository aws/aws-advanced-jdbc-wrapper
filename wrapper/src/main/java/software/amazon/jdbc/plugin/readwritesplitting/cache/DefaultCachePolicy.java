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

package software.amazon.jdbc.plugin.readwritesplitting.cache;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import software.amazon.jdbc.plugin.readwritesplitting.UnifiedReadWriteSplittingPlugin;

/**
 * Default {@link ConnectionCachePolicy}, reproducing the legacy
 * {@code AbstractReadWriteSplittingPlugin.getKeepAliveTimeout} behavior: pooled connections defer
 * their lifetime to the pool (deadline {@code 0}); non-pooled connections use
 * {@code cachedReaderKeepAliveTimeoutMs} (default {@code 0} = keep reusing indefinitely).
 *
 * <p>The clock is injectable via {@link NanoTimeSource} so deadline computation is unit-testable
 * without real waits.
 */
public class DefaultCachePolicy implements ConnectionCachePolicy {

  private final long keepAliveMs;
  private final NanoTimeSource timeSource;

  public DefaultCachePolicy(final Properties properties) {
    this(UnifiedReadWriteSplittingPlugin.CACHED_READER_KEEP_ALIVE_TIMEOUT.getLong(properties),
        NanoTimeSource.SYSTEM);
  }

  public DefaultCachePolicy(final long keepAliveMs, final NanoTimeSource timeSource) {
    this.keepAliveMs = keepAliveMs;
    this.timeSource = timeSource;
  }

  @Override
  public long keepAliveDeadlineNanos(final boolean fromPool) {
    if (fromPool) {
      // Let the connection pool handle the lifetime of the connection.
      return 0;
    }
    return this.keepAliveMs > 0
        ? this.timeSource.nanoTime() + TimeUnit.MILLISECONDS.toNanos(this.keepAliveMs)
        : 0;
  }
}
