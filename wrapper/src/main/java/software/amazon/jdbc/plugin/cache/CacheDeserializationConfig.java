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

package software.amazon.jdbc.plugin.cache;

/**
 * Immutable per-connection configuration for reading cached values back from the cache.
 * Held by {@link RemoteQueryCachePlugin} and threaded through {@link CachedResultSet} and
 * {@link CachedSQLXML} so that opt-in choices made on one connection do not affect any other.
 */
public final class CacheDeserializationConfig {

  /** Default configuration: URL deserialization and StreamSource are both rejected. */
  public static final CacheDeserializationConfig STRICT =
      new CacheDeserializationConfig(false, false);

  private final boolean allowUrl;
  private final boolean allowStreamSource;

  public CacheDeserializationConfig(final boolean allowUrl, final boolean allowStreamSource) {
    this.allowUrl = allowUrl;
    this.allowStreamSource = allowStreamSource;
  }

  public boolean isAllowUrl() {
    return this.allowUrl;
  }

  public boolean isAllowStreamSource() {
    return this.allowStreamSource;
  }
}
