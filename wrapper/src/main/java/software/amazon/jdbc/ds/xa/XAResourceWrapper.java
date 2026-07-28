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

package software.amazon.jdbc.ds.xa;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * A pass-through {@link XAResource} that delegates every operation to the target driver's
 * {@code XAResource} while tracking the XA-active state of the wrapper's connection.
 *
 * <p>The XA-active flag is maintained around the transaction <em>branch</em>, not per method:
 * {@link #start} marks the connection XA-active; {@link #prepare} does <em>not</em> clear it (the
 * branch is still live between prepare and commit/rollback); the terminal {@link #commit} and
 * {@link #rollback} clear it. This flag is what causes voluntary connection switches to be skipped
 * and failover to fail fast for the duration of the branch.
 *
 * <p>This wrapper is a plain delegator and is not routed through the plugin pipeline (XA control
 * calls do not need per-call plugin interception).
 */
public class XAResourceWrapper implements XAResource {

  private final XAResource target;
  private final XaTransactionStateCallback stateCallback;

  public XAResourceWrapper(final XAResource target, final XaTransactionStateCallback stateCallback) {
    this.target = target;
    this.stateCallback = stateCallback;
  }

  @Override
  public void start(final Xid xid, final int flags) throws XAException {
    this.target.start(xid, flags);
    // Includes TMNOFLAGS, TMJOIN, and TMRESUME - in all cases the connection is now enlisted.
    this.stateCallback.setXaTransactionActive(true);
  }

  @Override
  public void end(final Xid xid, final int flags) throws XAException {
    // The work association ends here, but the branch is still resolvable via commit/rollback, so
    // the XA-active flag is intentionally NOT cleared.
    this.target.end(xid, flags);
  }

  @Override
  public int prepare(final Xid xid) throws XAException {
    // The branch remains live between prepare and commit/rollback; do not clear the flag.
    return this.target.prepare(xid);
  }

  @Override
  public void commit(final Xid xid, final boolean onePhase) throws XAException {
    try {
      this.target.commit(xid, onePhase);
    } finally {
      this.stateCallback.setXaTransactionActive(false);
    }
  }

  @Override
  public void rollback(final Xid xid) throws XAException {
    try {
      this.target.rollback(xid);
    } finally {
      this.stateCallback.setXaTransactionActive(false);
    }
  }

  @Override
  public Xid[] recover(final int flag) throws XAException {
    // Recovery is driven by the transaction manager by Xid and must never be blocked or altered.
    return this.target.recover(flag);
  }

  @Override
  public void forget(final Xid xid) throws XAException {
    this.target.forget(xid);
  }

  @Override
  public boolean isSameRM(final XAResource xares) throws XAException {
    // Always report distinct resource managers so the transaction manager creates a separate branch
    // for each wrapped connection and performs a full two-phase commit, rather than trying to join
    // branches onto a single connection. Two AwsWrapperXADataSource connections are independent (and
    // may even be routed to different physical hosts by the wrapper's topology awareness), so they
    // must not be treated as a joinable single resource. This also avoids target drivers that do not
    // support XA START ... JOIN (for example MySQL Connector/J), which reject a join with XAER_INVAL.
    return false;
  }

  @Override
  public int getTransactionTimeout() throws XAException {
    return this.target.getTransactionTimeout();
  }

  @Override
  public boolean setTransactionTimeout(final int seconds) throws XAException {
    return this.target.setTransactionTimeout(seconds);
  }
}
