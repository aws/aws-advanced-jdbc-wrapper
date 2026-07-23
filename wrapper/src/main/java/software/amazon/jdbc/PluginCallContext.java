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

package software.amazon.jdbc;

import java.util.HashMap;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Per-JDBC-call context that flows through the plugin pipeline.
 * Plugins can store and retrieve attributes, similar to servlet request attributes.
 *
 * <p>The context is scoped to the {@link PluginServiceImpl} instance, which has the same
 * lifecycle as the wrapper connection. It is automatically cleared before each call begins.
 * Upstream plugins (e.g., SQL parsing) can populate attributes that downstream plugins
 * (e.g., encryption, caching) consume.
 *
 * <p>Access the context from any plugin via {@link PluginService#getCallContext()}.
 */
public class PluginCallContext {

  private final Map<String, Object> attributes = new HashMap<>();

  /**
   * Handle to the statement wrapper for the current call, allowing a plugin to rebind a bound
   * plain {@code Statement} to a routed connection. Set by the execute pipeline after {@link #reset()}
   * (the wrapper only receives the raw target statement as {@code methodInvokeOn}, so it cannot be
   * reached directly). Cleared on {@link #reset()} so it never leaks across calls.
   */
  private @Nullable Rebindable rebindHandle;

  /**
   * Clears all attributes from the context.
   */
  public void reset() {
    attributes.clear();
    this.rebindHandle = null;
  }

  /** Sets the rebind handle for the current call (see {@link #rebindHandle}). */
  public void setRebindHandle(final @Nullable Rebindable rebindHandle) {
    this.rebindHandle = rebindHandle;
  }

  /** Returns the rebind handle for the current call, or {@code null} if none. */
  public @Nullable Rebindable getRebindHandle() {
    return this.rebindHandle;
  }

  /**
   * Sets an attribute in the context.
   *
   * @param key the attribute key
   * @param value the attribute value
   */
  public void setAttribute(final String key, final Object value) {
    attributes.put(key, value);
  }

  /**
   * Gets an attribute from the context.
   *
   * @param key the attribute key
   * @param type the expected type
   * @param <T> the expected type
   * @return the attribute value, or null if not set
   * @throws ClassCastException if the value is not of the expected type
   */
  public <T> @Nullable T getAttribute(final String key, final Class<T> type) {
    Object value = attributes.get(key);
    if (value == null) {
      return null;
    }
    return type.cast(value);
  }

  /**
   * Checks if an attribute exists.
   *
   * @param key the attribute key
   * @return true if the attribute is set
   */
  public boolean hasAttribute(final String key) {
    return attributes.containsKey(key);
  }
}
