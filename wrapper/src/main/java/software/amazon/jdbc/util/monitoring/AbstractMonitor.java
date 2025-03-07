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

import java.util.logging.Logger;
import software.amazon.jdbc.util.Messages;

public abstract class AbstractMonitor implements Monitor, Runnable {
  private static Logger LOGGER = Logger.getLogger(AbstractMonitor.class.getName());
  private final MonitorService monitorService;
  protected long lastUsedTimestampNanos;
  protected MonitorState state;

  protected AbstractMonitor(MonitorService monitorService) {
    this.monitorService = monitorService;
  }

  @Override
  public void run() {
    try {
      start();
    } catch (Exception e) {
      LOGGER.fine(Messages.get("AbstractMonitor.unexpectedError", new Object[]{this, e}));
      this.state = MonitorState.ERROR;
      monitorService.handleMonitorError(this, e);
    }
  }

  @Override
  public long getLastUsedTimestampNanos() {
    return this.lastUsedTimestampNanos;
  }

  @Override
  public MonitorState getState() {
    return this.state;
  }
}
