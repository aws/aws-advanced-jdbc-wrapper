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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * LazyCleaner is a utility class that allows to register objects for deferred cleanup.
 *
 * <p>This is the Java 11+ implementation that uses the native {@link Cleaner} API
 * introduced in Java 9+ for deferred cleanup operations.</p>
 *
 * <p>This class replaces the Java 8 PhantomReference-based implementation via the
 * multi-release JAR mechanism when running on Java 11+.</p>
 *
 * <p>Note: this is a driver-internal class</p>
 */
public class LazyCleanerImpl implements LazyCleaner {
  private static final Logger LOGGER = Logger.getLogger(LazyCleanerImpl.class.getName());
  private static final LazyCleanerImpl instance =
      new LazyCleanerImpl(
          Duration.ofMillis(Long.getLong("aws.jdbc.cleanup.thread.ttl", 30000)),
          "AWS-JDBC-Cleaner"
      );

  private final Cleaner cleaner;

  /**
   * Returns a default cleaner instance.
   *
   * <p>Note: this is driver-internal API.</p>
   * @return the instance of LazyCleaner
   */
  public static LazyCleanerImpl getInstance() {
    return instance;
  }

  /**
   * Creates a LazyCleaner with the specified configuration.
   *
   * <p>Note: The {@code threadName} and {@code threadTtl} parameters are ignored
   * in this implementation since the JVM manages Cleaner threads internally.</p>
   *
   * @param threadTtl the maximum time the cleanup thread will wait (ignored, for API compatibility)
   * @param threadName the name for the cleanup thread (ignored, for API compatibility)
   */
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

  public <T extends Throwable> Cleanable<T> register(Object obj, CleaningAction<T> action) {
    assert obj != action : "object handle should not be the same as cleaning action, otherwise"
        + " the object will never become phantom reachable, so the action will never trigger";

    CleanableWrapper<T> wrapper = new CleanableWrapper<>(action);
    Cleaner.Cleanable nativeCleanable = cleaner.register(obj, wrapper::leakDetected);
    wrapper.setNativeCleanable(nativeCleanable);
    return wrapper;
  }

  private static class CleanableWrapper<T extends Throwable> implements Cleanable<T> {
    private java.lang.ref.Cleaner.Cleanable nativeCleanable;
    private volatile CleaningAction<T> action;

    CleanableWrapper(CleaningAction<T> action) {
      this.action = action;
    }

    void setNativeCleanable(java.lang.ref.Cleaner.Cleanable nativeCleanable) {
      this.nativeCleanable = nativeCleanable;
    }

    private synchronized CleaningAction<T> getCleaningAction() {
      CleaningAction<T> action = this.action;
      this.action = null;
      return action;
    }

    void leakDetected() {
      CleaningAction<T> cleaningAction = getCleaningAction();
      if (cleaningAction == null) {
        return;
      }
      try {
        cleaningAction.onClean(true);
      } catch (Throwable e) {
        if (e instanceof InterruptedException) {
          LOGGER.log(Level.WARNING, "Unexpected interrupt while executing onClean", e);
        } else {
          // Should not happen if cleaners are well-behaved
          LOGGER.log(Level.WARNING, "Unexpected exception while executing onClean", e);
        }      }
    }

    @Override
    public void clean() throws T {
      CleaningAction<T> cleaningAction = getCleaningAction();
      if (cleaningAction == null) {
        return;
      }

      if (nativeCleanable != null) {
        nativeCleanable.clean();
        nativeCleanable = null;
      }

      cleaningAction.onClean(false);
    }
  }
}
