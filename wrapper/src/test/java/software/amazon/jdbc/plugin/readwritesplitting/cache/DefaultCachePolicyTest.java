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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link DefaultCachePolicy}. */
public class DefaultCachePolicyTest {

  private static final long FIXED_NOW = 1_000_000_000L;
  private final NanoTimeSource fixedClock = () -> FIXED_NOW;

  @Test
  void pooledConnection_deadlineIsZero() {
    // Pooled connections defer their lifetime to the pool regardless of the configured timeout.
    final DefaultCachePolicy policy = new DefaultCachePolicy(5_000L, fixedClock);
    assertEquals(0, policy.keepAliveDeadlineNanos(true));
  }

  @Test
  void nonPooled_zeroTimeout_deadlineIsZero() {
    // Default timeout (0) means the reader is reused indefinitely (no deadline).
    final DefaultCachePolicy policy = new DefaultCachePolicy(0L, fixedClock);
    assertEquals(0, policy.keepAliveDeadlineNanos(false));
  }

  @Test
  void nonPooled_positiveTimeout_deadlineIsNowPlusTimeout() {
    final long timeoutMs = 5_000L;
    final DefaultCachePolicy policy = new DefaultCachePolicy(timeoutMs, fixedClock);
    final long expected = FIXED_NOW + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    assertEquals(expected, policy.keepAliveDeadlineNanos(false));
  }

  @Test
  void nonPooled_positiveTimeout_deadlineIsInTheFuture() {
    final DefaultCachePolicy policy = new DefaultCachePolicy(1L, fixedClock);
    assertTrue(policy.keepAliveDeadlineNanos(false) > FIXED_NOW);
  }
}
