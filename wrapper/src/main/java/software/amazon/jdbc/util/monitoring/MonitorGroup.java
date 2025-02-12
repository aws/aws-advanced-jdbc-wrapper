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

package software.amazon.jdbc.util.monitoring;

import java.util.Set;
import java.util.function.Supplier;
import software.amazon.jdbc.util.SlidingExpirationCacheWithCleanupThread;

public class MonitorGroup {
  private final String groupName;
  private final Supplier<Monitor> supplier;
  private final Set<MonitorErrorResponse> exceptionResponses;
  private final SlidingExpirationCacheWithCleanupThread<Object, Monitor> cache;

  public MonitorGroup(String groupName, Supplier<Monitor> supplier,
      Set<MonitorErrorResponse> exceptionResponses,
      SlidingExpirationCacheWithCleanupThread<Object, Monitor> cache) {
    this.groupName = groupName;
    this.supplier = supplier;
    this.exceptionResponses = exceptionResponses;
    this.cache = cache;
  }

  public String getGroupName() {
    return groupName;
  }

  public Supplier<Monitor> getSupplier() {
    return supplier;
  }

  public Set<MonitorErrorResponse> getExceptionResponses() {
    return exceptionResponses;
  }

  public SlidingExpirationCacheWithCleanupThread<Object, Monitor> getCache() {
    return cache;
  }

  public int size() {
    return cache.size();
  }
}
