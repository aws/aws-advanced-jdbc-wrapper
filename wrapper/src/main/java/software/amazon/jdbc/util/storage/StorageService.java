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

import java.util.Map;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.ItemDisposalFunc;
import software.amazon.jdbc.util.ShouldDisposeFunc;

public interface StorageService {
  /**
   * Register a new item category with the storage service. This method needs to be called before adding new categories
   * of items to the service, so that the service knows when and how to dispose of the item. Expected item categories
   * ("topology" and "customEndpoint") will be added automatically during driver initialization, but this method can be
   * called by users if they want to add a new category.
   *
   * @param itemCategory           a String representing the item category, eg "customEndpoint".
   * @param itemClass              the class of item that will be stored under this category, eg
   *                               `CustomEndpointInfo.class`.
   * @param isRenewableExpiration  controls whether the item's expiration should be renewed if the item is fetched,
   *                               regardless of whether it is already expired or not.
   * @param cleanupIntervalNanos   how often the item category should be cleaned of expired entries, in nanoseconds.
   * @param timeToLiveNanos        how long an item should be stored before being considered expired, in nanoseconds.
   * @param shouldDisposeFunc      a function defining whether an item should be disposed if expired. If null is passed,
   *                               the item will always be disposed if expired.
   * @param itemDisposalFunc       a function defining how to dispose of an item when it is removed. If null is
   *                               passed, the item will be removed without performing any additional operations.
   * @param <V>                    the type of item that will be stored under the item category.
   */
  <V> void registerItemCategoryIfAbsent(
      String itemCategory,
      Class<V> itemClass,
      boolean isRenewableExpiration,
      long cleanupIntervalNanos,
      long timeToLiveNanos,
      @Nullable ShouldDisposeFunc<V> shouldDisposeFunc,
      @Nullable ItemDisposalFunc<V> itemDisposalFunc);

  /**
   * Stores an item under the given category.
   *
   * @param itemCategory a String representing the item category, eg "customEndpoint".
   * @param key          the key for the item, eg "custom-endpoint.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:5432".
   * @param item         the item to store.
   * @param <V>          the type of the item being stored.
   */
  <V> void set(String itemCategory, Object key, V item);

  /**
   * Gets an item stored under the given category.
   *
   * @param itemCategory a String representing the item category, eg "customEndpoint".
   * @param key          the key for the item, eg "custom-endpoint.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:5432".
   * @param itemClass    the expected class of the item being retrieved, eg `CustomEndpointInfo.class`.
   * @param <V>          the type of the item being retrieved.
   * @return the item stored at the given key under the given category.
   */
  <V> @Nullable V get(String itemCategory, Object key, Class<V> itemClass);

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
   */
  void remove(String itemCategory, Object key);

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

  // TODO: this is only called by the suggestedClusterId logic in RdsHostListProvider, which will be removed. This
  //  method should potentially be removed at that point as well.
  <K, V> @Nullable Map<K, V> getEntries(String itemCategory);

  // TODO: this method is only called by tests. We should evaluate whether we want to keep it or not.
  int size(String itemCategory);
}
