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
/*
 * portions Copyright (c) 2022, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package software.amazon.jdbc.util;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Extends a ReentrantLock for use in try-with-resources block.
 *
 * <h2>Example use</h2>
 * <pre>{@code
 *
 *   try (ResourceLock ignore = lock.obtain()) {
 *     // do something while holding the resource lock
 *   }
 *
 * }</pre>
 */
public final class ResourceLock extends ReentrantLock implements AutoCloseable {

  /**
   * Obtain a lock and return the ResourceLock for use in try-with-resources block.
   */
  public ResourceLock obtain() {
    lock();
    return this;
  }

  /**
   * Unlock on exit of try-with-resources block.
   */
  @Override
  public void close() {
    this.unlock();
  }
}