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

package software.amazon.jdbc.ds.xa;

/**
 * Callback used by {@link XAResourceWrapper} to mark the current logical connection's
 * {@code PluginService} as XA-active (or not) as the transaction branch starts and completes.
 * Implemented by {@link XAConnectionWrapper}, which knows the currently checked-out handle.
 */
@FunctionalInterface
public interface XaTransactionStateCallback {

  /**
   * Marks the current connection XA-active or clears it.
   *
   * @param active true when a branch has started; false when it has committed/rolled back.
   */
  void setXaTransactionActive(boolean active);
}
