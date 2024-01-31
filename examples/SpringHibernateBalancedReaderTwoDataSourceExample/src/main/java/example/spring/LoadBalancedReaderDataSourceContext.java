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

package example.spring;

import java.util.concurrent.atomic.AtomicInteger;

public class LoadBalancedReaderDataSourceContext {

  private static final ThreadLocal<AtomicInteger> READER_DATASOURCE_LEVEL =
      ThreadLocal.withInitial(() -> new AtomicInteger(0));

  private LoadBalancedReaderDataSourceContext() {
  }

  public static boolean isLoadBalancedReaderZone() {
    return READER_DATASOURCE_LEVEL.get().get() > 0;
  }

  public static void enter() {
    READER_DATASOURCE_LEVEL.get().incrementAndGet();
  }

  public static void exit() {
    READER_DATASOURCE_LEVEL.get().decrementAndGet();
  }
}
