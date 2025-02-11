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

package software.amazon.jdbc.util.storage;

import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.ItemDisposalFunc;
import software.amazon.jdbc.util.ShouldDisposeFunc;

public interface StorageService {
  /**
   * Register a new item category with the storage service. This method needs to be called before adding new categories
   * of items to the storage service, so that the storage service knows when and how to dispose of the item. Expected
   * item categories ("topology" and "customEndpoint") will be added automatically during driver initialization, but
   * this method can be called by users if they want to add a new category.
   *
   * @param itemCategory           a String representing the item category, eg "customEndpoint".
   * @param itemClass              the class of item that will be stored under this category, eg `CustomEndpointInfo
   *                               .class`.
   * @param isRenewableExpiration  controls whether the item's expiration should be renewed if the item is fetched,
   *                               regardless of whether it is already expired or not.
   * @param cleanupIntervalNanos   how often the item category should be cleaned of expired entries, in nanoseconds.
   * @param expirationTimeoutNanos how long an item should be stored before expiring, in nanoseconds.
   * @param shouldDisposeFunc      a function defining whether an item should be disposed if expired. If null is passed,
   *                               the item will always be disposed if expired.
   * @param itemDisposalFunc       a function defining how to dispose of an item when it is removed. If null is
   *                               passed, the
   *                               item will be removed without performing any additional operations.
   */
  void registerItemCategoryIfAbsent(
      String itemCategory,
      Class<Object> itemClass,
      boolean isRenewableExpiration,
      long cleanupIntervalNanos,
      long expirationTimeoutNanos,
      @Nullable ShouldDisposeFunc<Object> shouldDisposeFunc,
      @Nullable ItemDisposalFunc<Object> itemDisposalFunc);

  /**
   * Stores an item under the given category.
   *
   * @param itemCategory a String representing the item category, eg "customEndpoint".
   * @param key          the key for the item, eg "custom-endpoint.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:5432".
   * @param item         the item to store.
   * @param <T>          the type of the item being stored.
   */
  <T> void set(String itemCategory, Object key, Object item);

  /**
   * Gets an item stored under the given category.
   *
   * @param itemCategory a String representing the item category, eg "customEndpoint".
   * @param key          the key for the item, eg "custom-endpoint.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:5432".
   * @param <T>          the type of the item being retrieved.
   * @return the item stored at the given key under the given category.
   */
  <T> @Nullable T get(String itemCategory, Object key);

  /**
   * Indicates whether an item exists under the given item category and key.
   *
   * @param itemCategory a String representing the item category, eg "customEndpoint".
   * @param key          the key for the item, eg "custom-endpoint.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:5432".
   * @return true if the item exists under the given item category and key, otherwise returns false.
   */
  boolean exists(String itemCategory, Object key);

  /**
   * Removes an item stored under the given category.
   *
   * @param itemCategory a String representing the item category, eg "customEndpoint".
   * @param key          the key for the item, eg "custom-endpoint.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:5432".
   * @param <T>          the type of the item being removed.
   * @return the item removed from the storage service.
   */
  <T> @Nullable T remove(String itemCategory, Object key);

  /**
   * Clears all items from the given category. For example, storageService.clear("customEndpoint") will remove all
   * custom endpoint information from the storage service.
   *
   * @param itemCategory a String representing the item category, eg "customEndpoint".
   */
  void clear(String itemCategory);

  /**
   * Clears all information from all categories of the storage service.
   */
  void clearAll();
}
