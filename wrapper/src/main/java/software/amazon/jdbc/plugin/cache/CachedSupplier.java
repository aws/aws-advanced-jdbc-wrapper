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

package software.amazon.jdbc.plugin.cache;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public final class CachedSupplier {

  private CachedSupplier() {
    throw new UnsupportedOperationException("Utility class should not be instantiated");
  }

  public static <T> Supplier<T> memoizeWithExpiration(
      Supplier<T> delegate, long duration, TimeUnit unit) {

    Objects.requireNonNull(delegate, "delegate Supplier must not be null");
    Objects.requireNonNull(unit, "TimeUnit must not be null");
    if (duration <= 0) {
      throw new IllegalArgumentException("duration must be > 0");
    }

    return new ExpiringMemoizingSupplier<>(delegate, duration, unit);
  }

  private static final class ExpiringMemoizingSupplier<T> implements Supplier<T> {

    private final Supplier<T> delegate;
    private final long durationNanos;
    private final ReentrantLock lock = new ReentrantLock();

    private volatile T value;
    private volatile long expirationNanos; // 0 means not yet initialized

    ExpiringMemoizingSupplier(Supplier<T> delegate, long duration, TimeUnit unit) {
      this.delegate = delegate;
      this.durationNanos = unit.toNanos(duration);
    }

    @Override
    public T get() {
      long now = System.nanoTime();

      // Check if value is expired or uninitialized
      if (expirationNanos == 0 || now - expirationNanos >= 0) {
        lock.lock();
        try {
          if (expirationNanos == 0 || now - expirationNanos >= 0) {
            value = delegate.get();
            long next = now + durationNanos;
            expirationNanos = (next == 0) ? 1 : next; // avoid 0 sentinel
          }
        } finally {
          lock.unlock();
        }
      }
      return value;
    }
  }
}
