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

package integration.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.logging.Logger;
import integration.container.tests.FailoverTest;

public class RetryHelper {

  private static final Logger LOGGER = Logger.getLogger(RetryHelper.class.getName());

  /**
   * Retries the given condition check until it returns true or the timeout is reached.
   *
   * @param timeoutMs the maximum time to wait in milliseconds
   * @param delayMs the delay between retries in milliseconds
   * @param condition a lambda that returns true when the retry loop should exit
   * @return true if the condition was met within the timeout, false otherwise
   */
  public static boolean retryUntil(long timeoutMs, long delayMs, BooleanSupplier condition) {
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);

    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return true;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(delayMs);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        return false;
      }
    }

    return false;
  }

  /**
   * Retries the given condition check until it returns true or the timeout is reached.
   *
   * @param condition a lambda that returns true when the retry loop should exit
   * @return true if the condition was met within the timeout, false otherwise
   */
  public static boolean retryUntil(BooleanSupplier condition) {
    return retryUntil(60000, 5000, condition);
  }

  public static boolean verifyWriter(AuroraTestUtility auroraUtil, String expectedWriterId) {
    AtomicReference<String> apiWriterId = new AtomicReference<>();
    return RetryHelper.retryUntil(TimeUnit.MINUTES.toMillis(5), 5000, () -> {
      apiWriterId.set(auroraUtil.getDBClusterWriterInstanceId());
      LOGGER.finest("Writer (API): " + apiWriterId.get());
      return expectedWriterId.equalsIgnoreCase(apiWriterId.get());
    });
  }
}
