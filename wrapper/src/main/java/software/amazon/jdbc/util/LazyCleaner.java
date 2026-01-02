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

public interface LazyCleaner {

  /**
   * Cleanable interface for objects that can be manually cleaned.
   *
   * @param <T> the type of exception that can be thrown during cleanup
   */
  interface Cleanable<T extends Throwable> {
    void clean() throws T;
  }

  /**
   * CleaningAction interface for cleanup actions that are notified whether cleanup
   * occurred due to a leak (automatic) or manual cleanup.
   *
   * @param <T> the type of exception that can be thrown during cleanup
   */
  interface CleaningAction<T extends Throwable> {
    void onClean(boolean leak) throws T;
  }

  /**
   * Registers an object for cleanup when it becomes phantom reachable.
   *
   * @param obj the object to monitor for cleanup (should not be the same as action)
   * @param action the action to perform when the object becomes unreachable
   * @param <T> the type of exception that can be thrown during cleanup
   * @return a Cleanable that can be used to manually trigger cleanup
   */
  <T extends Throwable> Cleanable<T> register(Object obj, CleaningAction<T> action);
}
