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

import org.checkerframework.checker.nullness.qual.Nullable;

public class MonitorStatus {
  private final MonitorState state;
  private final long lastUsedTimeNs;
  private final @Nullable Throwable exception;

  public MonitorStatus(MonitorState state, long lastUsedTimeNs, @Nullable Throwable exception) {
    this.state = state;
    this.lastUsedTimeNs = lastUsedTimeNs;
    this.exception = exception;
  }

  public MonitorState getState() {
    return state;
  }

  public long getLastUsedTimeNs() {
    return lastUsedTimeNs;
  }

  public @Nullable Throwable getException() {
    return this.exception;
  }
}
