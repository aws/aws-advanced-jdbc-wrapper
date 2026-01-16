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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ExpirationCacheTest {

  @Test
  public void testComputeIfAbsent() throws InterruptedException {
    ExpirationCache<String, DummyItem> cache =
        new ExpirationCache<>(
            false, TimeUnit.MILLISECONDS.toNanos(100), (item) -> true, DummyItem::close);
    String key = "key";
    DummyItem itemSpy = Mockito.spy(new DummyItem());
    DummyItem newDummyItem = new DummyItem();
    assertEquals(itemSpy, cache.computeIfAbsent(key, k -> itemSpy));
    // item is not absent, so the new value should not be stored.
    assertEquals(itemSpy, cache.computeIfAbsent(key, k -> newDummyItem));

    // wait for item to expire.
    TimeUnit.MILLISECONDS.sleep(150);
    assertEquals(newDummyItem, cache.computeIfAbsent(key, k -> newDummyItem));
    // wait briefly for cache to call the disposal method on the old item.
    TimeUnit.MILLISECONDS.sleep(100);
    verify(itemSpy, times(1)).close();
  }

  @Test
  public void testRenewableExpiration() throws InterruptedException {
    ExpirationCache<String, DummyItem> cache =
        new ExpirationCache<>(
            true, TimeUnit.MILLISECONDS.toNanos(100), (item) -> true, DummyItem::close);
    String key = "key";
    DummyItem item = new DummyItem();
    cache.put(key, item);
    assertEquals(item, cache.get(key));

    // wait for item to expire.
    TimeUnit.MILLISECONDS.sleep(150);

    assertEquals(item, cache.get(key));
  }

  @Test
  public void testNonRenewableExpiration() throws InterruptedException {
    ExpirationCache<String, DummyItem> cache =
        new ExpirationCache<>(
            false, TimeUnit.MILLISECONDS.toNanos(100), (item) -> true, DummyItem::close);
    String key = "key";
    DummyItem itemSpy = Mockito.spy(new DummyItem());
    cache.put(key, itemSpy);
    assertEquals(itemSpy, cache.get(key));

    // wait for item to expire.
    TimeUnit.MILLISECONDS.sleep(150);

    assertNull(cache.get(key));
    assertFalse(cache.exists(key));

    cache.removeExpiredEntries();
    verify(itemSpy, times(1)).close();
  }

  @Test
  public void testRemove() {
    ExpirationCache<String, DummyItem> cache =
        new ExpirationCache<>(
            true, TimeUnit.MILLISECONDS.toNanos(100), (item) -> true, DummyItem::close);
    String key = "key";
    DummyItem itemSpy = Mockito.spy(new DummyItem());
    cache.put(key, itemSpy);
    assertEquals(itemSpy, cache.get(key));

    DummyItem removedItem = cache.remove(key);
    assertEquals(itemSpy, removedItem);
    verify(itemSpy, times(1)).close();
  }

  static class DummyItem {
    protected void close() {
      // do nothing.
    }
  }
}
