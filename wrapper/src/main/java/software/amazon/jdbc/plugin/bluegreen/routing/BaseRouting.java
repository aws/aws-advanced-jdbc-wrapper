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

import java.util.concurrent.TimeUnit;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenStatus;
import software.amazon.jdbc.util.storage.StorageService;

public abstract class BaseRouting {

  protected static final long SLEEP_CHUNK = 50L;

  protected long getNanoTime() {
    return System.nanoTime();
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  protected void delay(long delayMs, BlueGreenStatus bgStatus, StorageService storageService, String bgdId)
      throws InterruptedException {

    long start = System.nanoTime();
    long end = start + TimeUnit.MILLISECONDS.toNanos(delayMs);
    long minDelay = Math.min(delayMs, SLEEP_CHUNK);

    if (bgStatus == null) {
      TimeUnit.MILLISECONDS.sleep(delayMs);
    } else {
      // Check whether bgStatus change, or until waited specified delay time.
      do {
        synchronized (bgStatus) {
          bgStatus.wait(minDelay);
        }
      } while (
        // check if status reference is changed
        bgStatus == storageService.get(BlueGreenStatus.class, bgdId)
            && System.nanoTime() < end
            && !Thread.currentThread().isInterrupted());
    }
  }
}
