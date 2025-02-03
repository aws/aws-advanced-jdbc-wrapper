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

public abstract class MonitorException {
  private final String monitorType;
  private final Object monitorKey;
  private final Throwable cause;

  public MonitorException(String monitorType, Object monitorKey, Throwable cause) {
    this.monitorType = monitorType;
    this.monitorKey = monitorKey;
    this.cause = cause;
  }

  public String getMonitorType() {
    return monitorType;
  }

  public Object getMonitorKey() {
    return monitorKey;
  }

  public Throwable getCause() {
    return cause;
  }
}
