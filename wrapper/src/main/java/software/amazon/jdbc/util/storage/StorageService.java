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
   * Registers a new item class with the storage service. This method needs to be called before adding new classes of
   * items to the service, so that the service knows when and how to dispose of the item. Expected item classes will be
   * added automatically during driver initialization, but this method can be called to add new classes of items.
   *
   * @param itemClass              the class of the item that will be stored, eg `CustomEndpointInfo.class`.
   * @param isRenewableExpiration  controls whether the item's expiration should be renewed if the item is fetched,
   *                               regardless of whether it is already expired or not.
   * @param timeToLiveNanos        how long an item should be stored before being considered expired, in nanoseconds.
   * @param shouldDisposeFunc      a function defining whether an item should be disposed if expired. If null is passed,
   *                               the item will always be disposed if expired.
   * @param itemDisposalFunc       a function defining how to dispose of an item when it is removed. If null is
   *                               passed, the item will be removed without performing any additional operations.
   * @param <V>                    the type of item that will be stored under the item class.
   */
  <V> void registerItemClassIfAbsent(
      Class<V> itemClass,
      boolean isRenewableExpiration,
      long timeToLiveNanos,
      @Nullable ShouldDisposeFunc<V> shouldDisposeFunc,
      @Nullable ItemDisposalFunc<V> itemDisposalFunc);

  /**
   * Stores an item in the storage service under the given item class.
   *
   * @param key          the key for the item, eg "custom-endpoint.cluster-custom-XYZ.us-east-2.rds.amazonaws.com".
   * @param item         the item to store.
   * @param <V>          the type of the item being retrieved.
   */
  <V> void set(Object key, V item);

  /**
   * Gets an item stored in the storage service.
   *
   * @param itemClass    the expected class of the item being retrieved, eg `CustomEndpointInfo.class`.
   * @param key          the key for the item, eg "custom-endpoint.cluster-custom-XYZ.us-east-2.rds.amazonaws.com".
   * @param <V>          the type of the item being retrieved.
   * @return the item stored at the given key for the given item class.
   */
  <V> @Nullable V get(Class<V> itemClass, Object key);

  /**
   * Indicates whether an item exists under the given item class and key.
   *
   * @param itemClass    the class of the item.
   * @param key          the key for the item, eg "custom-endpoint.cluster-custom-XYZ.us-east-2.rds.amazonaws.com".
   * @return true if the item exists under the given item class and key, otherwise returns false.
   */
  boolean exists(Class<?> itemClass, Object key);

  /**
   * Removes an item stored under the given item class.
   *
   * @param itemClass    the class of the item.
   * @param key          the key for the item, eg "custom-endpoint.cluster-custom-XYZ.us-east-2.rds.amazonaws.com".
   */
  void remove(Class<?> itemClass, Object key);

  /**
   * Clears all items of the given item class. For example, storageService.clear(AllowedAndBlockedHosts.class) will
   * remove all AllowedAndBlockedHost items from the storage service.
   *
   * @param itemClass the class of the items to clear.
   */
  void clear(Class<?> itemClass);

  /**
   * Clears all items from the storage service.
   */
  void clearAll();

  // TODO: this is only called by the suggestedClusterId logic in RdsHostListProvider, which will be removed. This
  //  method should potentially be removed at that point as well.
  <K, V> @Nullable Map<K, V> getEntries(Class<?> itemClass);

  int size(Class<?> itemClass);
}
