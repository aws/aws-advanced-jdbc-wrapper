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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import software.amazon.jdbc.plugin.customendpoint.CustomEndpointMonitorImpl;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

/**
 * An AbstractMonitor that implements common monitor logic.
 */
public abstract class AbstractMonitor implements Monitor, Runnable {
  private static final Logger LOGGER = Logger.getLogger(AbstractMonitor.class.getName());
  protected final AtomicBoolean stop = new AtomicBoolean(false);
  protected final long terminationTimeoutSec;
  protected final CoreMonitorService monitorService;
  protected final ExecutorService monitorExecutor = Executors.newSingleThreadExecutor(runnableTarget -> {
    final Thread monitoringThread = new Thread(runnableTarget);
    monitoringThread.setDaemon(true);
    if (!StringUtils.isNullOrEmpty(monitoringThread.getName())) {
      monitoringThread.setName(monitoringThread.getName() + "-" + getMonitorNameSuffix());
    }
    return monitoringThread;
  });

  protected long lastActivityTimestampNanos;
  protected MonitorState state;

  protected AbstractMonitor(CoreMonitorService monitorService, long terminationTimeoutSec) {
    this.monitorService = monitorService;
    this.terminationTimeoutSec = terminationTimeoutSec;
    this.lastActivityTimestampNanos = System.nanoTime();
  }

  @Override
  public void start() {
    this.monitorExecutor.submit(this);
    this.monitorExecutor.shutdown();
  }

  /**
   * Starts the monitor workflow, making sure to set the initial state of the monitor. The monitor's workflow is wrapped
   * in a try-catch so that unexpected exceptions are reported to the monitor service and the monitor's state is updated
   * to {@link MonitorState#ERROR}.
   */
  @Override
  public void run() {
    try {
      LOGGER.finest(Messages.get("AbstractMonitor.startingMonitor", new Object[] {this}));
      this.state = MonitorState.RUNNING;
      this.lastActivityTimestampNanos = System.nanoTime();
      monitor();
    } catch (Exception e) {
      LOGGER.fine(Messages.get("AbstractMonitor.unexpectedError", new Object[] {this, e}));
      this.state = MonitorState.ERROR;
      // monitorService.reportMonitorError(this, e);
    }
  }

  @Override
  public void stop() {
    LOGGER.fine(Messages.get("AbstractMonitor.stoppingMonitor", new Object[] {this}));
    this.stop.set(true);

    try {
      if (!this.monitorExecutor.awaitTermination(this.terminationTimeoutSec, TimeUnit.SECONDS)) {
        LOGGER.info(Messages.get(
            "AbstractMonitor.monitorTerminationTimeout", new Object[] {terminationTimeoutSec, this}));
        this.monitorExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      LOGGER.info(Messages.get("AbstractMonitor.interruptedWhileTerminating", new Object[] {this}));
      Thread.currentThread().interrupt();
      this.monitorExecutor.shutdownNow();
    } finally {
      close();
      this.state = MonitorState.STOPPED;
    }
  }

  @Override
  public void close() {
    // do nothing. Classes that extend this class should override this method if they open resources that need closing.
  }

  @Override
  public long getLastActivityTimestampNanos() {
    return this.lastActivityTimestampNanos;
  }

  @Override
  public MonitorState getState() {
    return this.state;
  }

  @Override
  public boolean canDispose() {
    return true;
  }

  /**
   * Forms the suffix for the monitor thread name by abbreviating the concrete class name. For example, a
   * {@link CustomEndpointMonitorImpl} will have a suffix of "cemi".
   *
   * @return the suffix for the monitor thread name.
   */
  private String getMonitorNameSuffix() {
    return this.getClass().getSimpleName().replaceAll("[a-z]", "").toLowerCase();
  }
}
