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

package com.amazon.awslabs.jdbc.cleanup;

public interface CanReleaseResources {

  /**
   * An object that implements this interface should release all acquired resources assuming it may
   * be disposed at any time. This method may be called more than once.
   *
   * <p>Calling this method does NOT mean that an object is disposing and there won't be any further
   * calls. An object should keep its functional state after calling this method.
   */
  void releaseResources();
}
