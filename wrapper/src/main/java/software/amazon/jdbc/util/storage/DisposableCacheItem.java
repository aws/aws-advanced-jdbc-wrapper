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

import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;

public class DisposableCacheItem<V> extends CacheItem<V> {
  protected @Nullable final ShouldDisposeFunc<V> shouldDisposeFunc;

  /**
   * Constructs a CacheItem.
   *
   * @param item                the item value.
   * @param expirationTimeNanos the time at which the CacheItem should be considered expired.
   */
  protected DisposableCacheItem(@NotNull V item, long expirationTimeNanos) {
    super(item, expirationTimeNanos);
    this.shouldDisposeFunc = null;
  }

  /**
   * Constructs a CacheItem.
   *
   * @param item                the item value.
   * @param expirationTimeNanos the time at which the CacheItem should be considered expired.
   */
  protected DisposableCacheItem(
      @NotNull V item,
      long expirationTimeNanos,
      @Nullable ShouldDisposeFunc<V> shouldDisposeFunc) {
    super(item, expirationTimeNanos);
    this.shouldDisposeFunc = shouldDisposeFunc;
  }

  /**
   * Determines if a cache item should be cleaned up. An item should be cleaned up if it has past its expiration time
   * and the {@link ShouldDisposeFunc} (if defined) indicates that it should be cleaned up.
   *
   * @return true if the cache item should be cleaned up. Otherwise, returns false.
   */
  protected boolean shouldCleanup() {
    final boolean isExpired = this.expirationTimeNanos != 0 && System.nanoTime() > this.expirationTimeNanos;
    if (shouldDisposeFunc != null) {
      return isExpired && shouldDisposeFunc.shouldDispose(this.item);
    }
    return isExpired;
  }

}
