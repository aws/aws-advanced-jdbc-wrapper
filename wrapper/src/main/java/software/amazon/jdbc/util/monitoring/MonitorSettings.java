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

import java.util.Set;
import org.checkerframework.checker.nullness.qual.NonNull;

public class MonitorSettings {
  private final long expirationTimeoutNanos;
  private final long inactiveTimeoutNanos;
  private @NonNull final Set<MonitorErrorResponse> errorResponses;

  public MonitorSettings(
      long expirationTimeoutNanos, long inactiveTimeoutNanos, @NonNull Set<MonitorErrorResponse> errorResponses) {
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

  public @NonNull Set<MonitorErrorResponse> getErrorResponses() {
    return errorResponses;
  }
}
