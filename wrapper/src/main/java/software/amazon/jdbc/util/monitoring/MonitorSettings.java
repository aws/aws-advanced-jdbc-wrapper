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

package software.amazon.jdbc.util.monitoring;

import java.util.EnumSet;
import java.util.Set;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A class defining settings for a monitor or monitor type.
 */
public class MonitorSettings {
  private final long expirationTimeoutNanos;
  private final long inactiveTimeoutNanos;
  private @Nullable final EnumSet<MonitorErrorResponse> errorResponses;

  /**
   * Constructs a MonitorSettings instance.
   *
   * @param expirationTimeoutNanos the amount of time that a monitor should sit in a cache before being considered
   *                               expired.
   * @param inactiveTimeoutNanos   a duration in nanoseconds defining the maximum amount of time that a monitor should
   *                               take between updating its last-updated timestamp. If a monitor has not updated its
   *                               last-updated timestamp within this duration it will be considered stuck.
   * @param errorResponses         a {@link Set} defining actions to take if the monitor is in an error state. If null,
   *                               no action will be performed.
   */
  public MonitorSettings(
      long expirationTimeoutNanos, long inactiveTimeoutNanos, @NonNull EnumSet<MonitorErrorResponse> errorResponses) {
    this.expirationTimeoutNanos = expirationTimeoutNanos;
    this.inactiveTimeoutNanos = inactiveTimeoutNanos;
    this.errorResponses = errorResponses;
  }

  public long getExpirationTimeoutNanos() {
    return expirationTimeoutNanos;
  }

  public long getInactiveTimeoutNanos() {
    return inactiveTimeoutNanos;
  }

  public @Nullable EnumSet<MonitorErrorResponse> getErrorResponses() {
    return errorResponses;
  }
}
