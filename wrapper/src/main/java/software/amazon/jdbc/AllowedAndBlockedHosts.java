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

import java.util.Collections;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.Utils;

/**
 * Represents the allowed and blocked hosts for connections.
 */
public class AllowedAndBlockedHosts {
  @Nullable private final Set<String> allowedHostIds;
  @Nullable private final Set<String> blockedHostIds;

  /**
   * Constructs an AllowedAndBlockedHosts instance with the specified allowed and blocked host IDs.
   *
   * @param allowedHostIds The set of allowed host IDs for connections. If null or empty, all host IDs that are not in
   *                       {@code blockedHostIds} are allowed.
   * @param blockedHostIds The set of blocked host IDs for connections. If null or empty, all host IDs in
   *                       {@code allowedHostIds} are allowed. If {@code allowedHostIds} is also null or empty, there
   *                       are no restrictions on which hosts are allowed.
   */
  public AllowedAndBlockedHosts(@Nullable Set<String> allowedHostIds, @Nullable Set<String> blockedHostIds) {
    this.allowedHostIds = Utils.isNullOrEmpty(allowedHostIds) ? null : Collections.unmodifiableSet(allowedHostIds);
    this.blockedHostIds = Utils.isNullOrEmpty(blockedHostIds) ? null : Collections.unmodifiableSet(blockedHostIds);
  }

  /**
   * Returns the set of allowed host IDs for connections. If null or empty, all host IDs that are not in
   * {@code blockedHostIds} are allowed.
   *
   * @return the set of allowed host IDs for connections.
   */
  @Nullable
  public Set<String> getAllowedHostIds() {
    return this.allowedHostIds;
  }

  /**
   * Returns the set of blocked host IDs for connections. If null or empty, all host IDs in {@code allowedHostIds} are
   * allowed. If {@code allowedHostIds} is also null or empty, there are no restrictions on which hosts are allowed.
   *
   * @return the set of blocked host IDs for connections.
   */
  @Nullable
  public Set<String> getBlockedHostIds() {
    return this.blockedHostIds;
  }
}
