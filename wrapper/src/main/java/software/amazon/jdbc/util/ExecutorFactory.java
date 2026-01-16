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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class ExecutorFactory {
  private static final ConcurrentHashMap<String, ThreadFactory> THREAD_FACTORY_MAP =
      new ConcurrentHashMap<>();

  public static ExecutorService newSingleThreadExecutor(String threadName) {
    return Executors.newSingleThreadExecutor(getThreadFactory(threadName));
  }

  public static ExecutorService newCachedThreadPool(String threadName) {
    return Executors.newCachedThreadPool(getThreadFactory(threadName));
  }

  public static ExecutorService newFixedThreadPool(int threadCount, String threadName) {
    return Executors.newFixedThreadPool(threadCount, getThreadFactory(threadName));
  }

  public static ScheduledExecutorService newSingleThreadScheduledThreadExecutor(String threadName) {
    return Executors.newSingleThreadScheduledExecutor(getThreadFactory(threadName));
  }

  private static ThreadFactory getThreadFactory(String threadName) {
    return THREAD_FACTORY_MAP.computeIfAbsent(threadName, ExecutorFactory::createThreadFactory);
  }

  private static ThreadFactory createThreadFactory(String threadName) {
    AtomicLong threadCounter = new AtomicLong();
    return runnable -> {
      String formattedThreadName =
          String.format(
              "%s %s-%d", "AWS Advanced JDBC Wrapper", threadName, threadCounter.incrementAndGet());
      Thread thread = new Thread(runnable, formattedThreadName);
      thread.setDaemon(true);
      return thread;
    };
  }
}
