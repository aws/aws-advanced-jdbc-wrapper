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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class SlidingExpirationCacheWithCleanupThread<K, V> extends SlidingExpirationCache<K, V> {

  private static final Logger LOGGER =
      Logger.getLogger(SlidingExpirationCacheWithCleanupThread.class.getName());

  protected final ExecutorService cleanupThreadPool = Executors.newFixedThreadPool(1, runnableTarget -> {
    final Thread monitoringThread = new Thread(runnableTarget);
    monitoringThread.setDaemon(true);
    return monitoringThread;
  });
  protected final ReentrantLock initLock = new ReentrantLock();
  protected boolean isInitialized = false;

  public SlidingExpirationCacheWithCleanupThread() {
    super();
    this.initCleanupThread();
  }

  public SlidingExpirationCacheWithCleanupThread(
      final ShouldDisposeFunc<V> shouldDisposeFunc,
      final ItemDisposalFunc<V> itemDisposalFunc) {
    super(shouldDisposeFunc, itemDisposalFunc);
    this.initCleanupThread();
  }

  public SlidingExpirationCacheWithCleanupThread(
      final ShouldDisposeFunc<V> shouldDisposeFunc,
      final ItemDisposalFunc<V> itemDisposalFunc,
      final long cleanupIntervalNanos) {
    super(shouldDisposeFunc, itemDisposalFunc, cleanupIntervalNanos);
    this.initCleanupThread();
  }

  protected void initCleanupThread() {
    if (!isInitialized) {
      initLock.lock();
      try {
        if (!isInitialized) {
          cleanupThreadPool.submit(() -> {
            while (true) {
              TimeUnit.NANOSECONDS.sleep(this.cleanupIntervalNanos);

              LOGGER.finest("Cleaning up...");
              this.cleanupTimeNanos.set(System.nanoTime() + cleanupIntervalNanos);
              cache.forEach((key, value) -> {
                try {
                  removeIfExpired(key);
                } catch (Exception ex) {
                  // ignore
                }
              });
            }
          });
          cleanupThreadPool.shutdown();
          isInitialized = true;
        }
      } finally {
        initLock.unlock();
      }
    }
  }

  @Override
  protected void cleanUp() {
    // Intentionally do nothing. Cleanup thread does the job.
  }
}
