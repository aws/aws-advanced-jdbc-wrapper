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
  protected long lastUsedTimestampNanos;
  protected MonitorState state;
  protected Exception unhandledException;

  @Override
  public void run() {
    try {
      execute();
    } catch (Exception e) {
      LOGGER.fine(Messages.get("AbstractMonitor.unexpectedError", new Object[]{this, this.unhandledException}));
      this.state = MonitorState.ERROR;
      this.unhandledException = e;
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

  @Override
  public Exception getUnhandledException() {
    return this.unhandledException;
  }
}
