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

package software.amazon.jdbc.util.storage;

import java.util.concurrent.TimeUnit;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.ItemDisposalFunc;
import software.amazon.jdbc.util.ShouldDisposeFunc;

public class ExpirationCacheBuilder<V> {
  protected @NonNull Class<V> valueClass;
  protected boolean isRenewableExpiration = false;
  protected long cleanupIntervalNanos = TimeUnit.MINUTES.toNanos(10);
  protected long timeToLiveNanos = TimeUnit.MINUTES.toNanos(5);
  protected @Nullable ShouldDisposeFunc<V> shouldDisposeFunc;
  protected @Nullable ItemDisposalFunc<V> itemDisposalFunc;

  public ExpirationCacheBuilder(@NonNull Class<V> valueClass) {
    this.valueClass = valueClass;
  }

  public ExpirationCacheBuilder<V> withIsRenewableExpiration(boolean isRenewableExpiration) {
    this.isRenewableExpiration = isRenewableExpiration;
    return this;
  }

  public ExpirationCacheBuilder<V> withCleanupIntervalNanos(long cleanupIntervalNanos) {
    this.cleanupIntervalNanos = cleanupIntervalNanos;
    return this;
  }

  public ExpirationCacheBuilder<V> withTimeToLiveNanos(long timeToLiveNanos) {
    this.timeToLiveNanos = timeToLiveNanos;
    return this;
  }

  public ExpirationCacheBuilder<V> withShouldDisposeFunc(ShouldDisposeFunc<V> shouldDisposeFunc) {
    this.shouldDisposeFunc = shouldDisposeFunc;
    return this;
  }

  public ExpirationCacheBuilder<V> withItemDisposalFunc(ItemDisposalFunc<V> itemDisposalFunc) {
    this.itemDisposalFunc = itemDisposalFunc;
    return this;
  }

  public ExpirationCache<Object, V> build() {
    return new ExpirationCache<>(
        this.valueClass,
        this.isRenewableExpiration,
        this.cleanupIntervalNanos,
        this.timeToLiveNanos,
        this.shouldDisposeFunc,
        this.itemDisposalFunc);
  }
}
