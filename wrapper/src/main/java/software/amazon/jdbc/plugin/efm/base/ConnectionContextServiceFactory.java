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

package software.amazon.jdbc.plugin.efm.base;

/**
 * Factory for creating {@link ConnectionContextService} instances based on system configuration.
 *
 * <p>The factory reads the system property {@value #POOLING_ENABLED_PROPERTY} to determine
 * which implementation to create:
 * <ul>
 *   <li>{@code true} (default): Creates a {@link PoolConnectionContextServiceImpl} that pools
 *       and reuses context objects</li>
 *   <li>{@code false}: Creates a {@link SimpleConnectionContextServiceImpl} that creates new
 *       context instances on each acquire call</li>
 * </ul>
 */
public final class ConnectionContextServiceFactory {

  public static final String POOLING_ENABLED_PROPERTY = "efm.contextPool.enabled";
  private static final boolean DEFAULT_POOLING_ENABLED = true;

  private ConnectionContextServiceFactory() { }

  public static ConnectionContextService getInstance() {
    final boolean poolingEnabled = Boolean.parseBoolean(
        System.getProperty(POOLING_ENABLED_PROPERTY, String.valueOf(DEFAULT_POOLING_ENABLED)));

    if (poolingEnabled) {
      return new PoolConnectionContextServiceImpl();
    }
    return new SimpleConnectionContextServiceImpl();
  }
}
