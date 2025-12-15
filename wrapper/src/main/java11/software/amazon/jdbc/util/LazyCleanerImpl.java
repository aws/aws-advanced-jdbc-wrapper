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

import java.lang.ref.Cleaner;
import java.time.Duration;
import java.util.concurrent.ThreadFactory;

/**
 * Java 11+ implementation using java.lang.ref.Cleaner.
 */
public class LazyCleanerImpl implements LazyCleaner {
  private static final LazyCleanerImpl instance =
      new LazyCleanerImpl(
          Duration.ofMillis(Long.getLong("aws.jdbc.cleanup.thread.ttl", 30000)),
          "AWS-JDBC-Cleaner"
      );

  private final Cleaner cleaner;

  public static LazyCleanerImpl getInstance() {
    return instance;
  }

  public LazyCleanerImpl(Duration threadTtl, final String threadName) {
    this(threadTtl, runnable -> {
      Thread thread = new Thread(runnable, threadName);
      thread.setDaemon(true);
      return thread;
    });
  }

  private LazyCleanerImpl(Duration threadTtl, ThreadFactory threadFactory) {
    this.cleaner = Cleaner.create(threadFactory);
  }

  public Cleanable register(Object obj, CleaningAction action) {
    return new CleanableWrapper(cleaner.register(obj, () -> {
      try {
        action.onClean(true);
      } catch (Throwable e) {
        // Cleaner swallows exceptions, but we should at least log them
        // The logging is handled by the action itself
      }
    }), action);
  }

  private static class CleanableWrapper implements Cleanable {
    private final java.lang.ref.Cleaner.Cleanable cleanable;
    private final CleaningAction action;
    private volatile boolean cleaned = false;

    CleanableWrapper(java.lang.ref.Cleaner.Cleanable cleanable, CleaningAction action) {
      this.cleanable = cleanable;
      this.action = action;
    }

    @Override
    public void clean() throws Exception {
      if (!cleaned) {
        cleaned = true;
        cleanable.clean();
        action.onClean(false);
      }
    }
  }
}
