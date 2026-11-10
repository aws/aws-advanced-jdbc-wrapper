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

package software.amazon.jdbc.plugin.encryption.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;

public class DataKeyCacheTest {

  private final DataKeyCache cache =
      new DataKeyCache(EncryptionConfig.builder().kmsRegion("us-east-1").build());

  @AfterEach
  void tearDown() {
    cache.shutdown();
  }

  @Test
  void put_replacingAnEntry_zeroizesTheReplacedKey() throws Exception {
    cache.put("key", filled((byte) 1));
    final byte[] storedFirstKey = storedKey("key");

    cache.put("key", filled((byte) 2));

    assertArrayEquals(new byte[32], storedFirstKey);
    assertArrayEquals(filled((byte) 2), cache.get("key"));
  }

  @Test
  void put_storesACopyOfTheCallersKey() {
    final byte[] key = filled((byte) 1);

    cache.put("key", key);
    Arrays.fill(key, (byte) 0);

    assertArrayEquals(filled((byte) 1), cache.get("key"));
  }

  /** Returns the array held by the cache entry itself, not the copy that get() hands out. */
  private byte[] storedKey(String keyId) throws Exception {
    final Field cacheField = DataKeyCache.class.getDeclaredField("cache");
    cacheField.setAccessible(true);
    final Object entry = ((Map<?, ?>) cacheField.get(cache)).get(keyId);
    final Field dataKeyField = entry.getClass().getDeclaredField("dataKey");
    dataKeyField.setAccessible(true);
    return (byte[]) dataKeyField.get(entry);
  }

  private static byte[] filled(byte value) {
    final byte[] key = new byte[32];
    Arrays.fill(key, value);
    return key;
  }
}
