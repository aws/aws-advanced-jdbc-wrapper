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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class CacheMapTest {

  private static final long LONG_TTL_NANOS = TimeUnit.MINUTES.toNanos(10);
  private static final long SHORT_TTL_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

  @Test
  public void testGetReturnsStoredValue() {
    final CacheMap<String, String> cacheMap = new CacheMap<>();
    cacheMap.put("key", "value", LONG_TTL_NANOS);

    assertEquals("value", cacheMap.get("key"));
  }

  @Test
  public void testGetReturnsNullForAbsentKey() {
    final CacheMap<String, String> cacheMap = new CacheMap<>();

    assertNull(cacheMap.get("key"));
  }

  @Test
  public void testGetRemovesExpiredEntry() throws InterruptedException {
    final CacheMap<String, String> cacheMap = new CacheMap<>();
    cacheMap.put("key", "value", SHORT_TTL_NANOS);
    assertEquals(1, cacheMap.size());

    // wait for item to expire.
    TimeUnit.MILLISECONDS.sleep(100);

    assertNull(cacheMap.get("key"));
    assertFalse(cacheMap.cache.containsKey("key"));
    assertEquals(0, cacheMap.size());
  }

  @Test
  public void testGetReturnsNewValueAfterExpiredEntryIsReplaced() throws InterruptedException {
    final CacheMap<String, String> cacheMap = new CacheMap<>();
    cacheMap.put("key", "old", SHORT_TTL_NANOS);

    // wait for item to expire.
    TimeUnit.MILLISECONDS.sleep(100);
    assertNull(cacheMap.get("key"));

    cacheMap.put("key", "new", LONG_TTL_NANOS);
    assertEquals("new", cacheMap.get("key"));
  }

  @Test
  public void testGetKeepsEntryRefreshedWhileExpiredEntryWasBeingChecked() {
    final CacheMap<String, String> cacheMap = new CacheMap<>();
    final String value = "value";
    final RefreshingCacheItem staleItem = new RefreshingCacheItem(cacheMap, "key", value);
    cacheMap.cache.put("key", staleItem);

    // The first expiry check reports the item as expired and, as another thread would, replaces it with a fresh
    // entry that holds an equal value. Only the entry that was actually expired may be removed.
    assertEquals(value, cacheMap.get("key"));

    assertTrue(staleItem.refreshed);
    assertEquals(1, cacheMap.size());
    assertSame(staleItem.freshItem, cacheMap.cache.get("key"));
    assertEquals(value, cacheMap.get("key"));
  }

  /**
   * A cache item that, on its first expiry check, reports itself as expired after another entry with an equal value
   * has been stored under the same key. This relies on CacheMap.get() calling isExpired() outside of
   * computeIfPresent; storing into the map from inside a remapping function would fail.
   */
  private static class RefreshingCacheItem extends CacheItem<String> {
    private final CacheMap<String, String> cacheMap;
    private final String key;
    private final CacheItem<String> freshItem;
    private boolean refreshed = false;

    RefreshingCacheItem(final CacheMap<String, String> cacheMap, final String key, final String value) {
      super(value, System.nanoTime() + LONG_TTL_NANOS);
      this.cacheMap = cacheMap;
      this.key = key;
      this.freshItem = new CacheItem<>(value, System.nanoTime() + LONG_TTL_NANOS);
    }

    @Override
    protected boolean isExpired() {
      if (!refreshed) {
        refreshed = true;
        cacheMap.cache.put(key, freshItem);
        return true;
      }
      return super.isExpired();
    }
  }
}
