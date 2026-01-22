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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.jdbc.exceptions.UnclosedConnectionException;
import software.amazon.jdbc.util.LazyCleaner;
import software.amazon.jdbc.util.LazyCleanerImpl;
import software.amazon.jdbc.util.Messages;

public class AtomicConnection {

  private static final Logger LOGGER = Logger.getLogger(AtomicConnection.class.getName());

  protected final AtomicReference<Connection> atomicReference = new AtomicReference<>();
  protected boolean logUnclosedConnections = false;
  protected LazyCleaner.Cleanable cleanable;
  protected ConnectionCleanupAction cleanupAction;

  public AtomicConnection(final Object referent) {
    this(referent, false);
  }

  public AtomicConnection(final Object referent, boolean logUnclosedConnections) {
    this.logUnclosedConnections = logUnclosedConnections;
    this.cleanable = LazyCleanerImpl.getInstance().register(
        referent,
        this.cleanupAction = new ConnectionCleanupAction(this.atomicReference, this.logUnclosedConnections));
  }

  public void clean() {
    this.cleanupAction = null;
    try {
      this.cleanable.clean();
    } catch (Exception e) {
      // Ignore
    }
  }

  public Connection get() {
    return this.atomicReference.get();
  }

  public void set(final Connection connection, final boolean closePreviousConnection) {
    this.set(connection, null, closePreviousConnection);
  }

  public void set(final Connection connection) {
    this.set(connection, null, true);
  }

  public void set(
      final Connection connection,
      final Throwable openConnectionStacktrace,
      final boolean closePreviousConnection) {

    if (this.cleanupAction == null) {
      throw new IllegalStateException(Messages.get("AtomicConnection.alreadyClean"));
    }

    Connection prevConnection = null;
    try {
      prevConnection = this.atomicReference.getAndSet(connection);

      if (this.logUnclosedConnections) {
        this.cleanupAction.setOpenConnectionStacktrace(
            connection == null
                ? null
                : (openConnectionStacktrace == null
                    ? new UnclosedConnectionException()
                    : new UnclosedConnectionException(openConnectionStacktrace)));
      } else {
        this.cleanupAction.setOpenConnectionStacktrace(null);
      }
    } finally {
      if (closePreviousConnection && prevConnection != null && prevConnection != connection) {
        try {
          prevConnection.close();
        } catch (SQLException e) {
          // Ignore
        }
      }
    }
  }

  public boolean compareAndSet(Connection expected, Connection updated) {
    final Connection prevConnection = this.atomicReference.get();
    boolean result = false;
    try {
      result = this.atomicReference.compareAndSet(expected, updated);
    } finally {
      if (result && prevConnection != null && prevConnection != updated) {
        try {
          prevConnection.close();
        } catch (SQLException e) {
          // Ignore
        }
      }
    }
    return result;
  }

  public static class ConnectionCleanupAction implements LazyCleaner.CleaningAction {
    private Throwable openConnectionStacktrace;
    private String threadName;
    private AtomicReference<Connection> atomicReference;
    protected boolean logUnclosedConnections = false;
    private volatile boolean cleaned = false;

    ConnectionCleanupAction(
        final AtomicReference<Connection> atomicReference,
        boolean logUnclosedConnections) {
      this.atomicReference = atomicReference;
      this.logUnclosedConnections = logUnclosedConnections;
      this.threadName = Thread.currentThread().getName();
    }

    public void setOpenConnectionStacktrace(Throwable openConnectionStacktrace) {
      this.openConnectionStacktrace = openConnectionStacktrace;
      this.threadName = Thread.currentThread().getName();
    }

    @Override
    public void onClean(boolean leak) {
      if (this.cleaned) {
        return;
      }
      this.cleaned = true;

      final Connection prevConnection = this.atomicReference.getAndSet(null);

      if (prevConnection == null) {
        this.atomicReference = null;
        this.openConnectionStacktrace = null;
        return;
      }

      if (leak && this.logUnclosedConnections) {
        LOGGER.log(
            Level.WARNING,
            this.openConnectionStacktrace,
            () -> Messages.get("AtomicConnection.finalizingUnclosedConnection", new Object[] {this.threadName}));
      }

      try {
        prevConnection.close();
      } catch (SQLException e) {
        // Ignore
      }

      this.atomicReference = null;
      this.openConnectionStacktrace = null;
    }
  }
}
