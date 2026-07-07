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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicBoolean;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link XAResourceWrapper}: delegation and XA-active flag lifecycle. */
public class XAResourceWrapperTest {

  private AutoCloseable closeable;

  @Mock private XAResource target;
  @Mock private Xid xid;

  private final AtomicBoolean xaActive = new AtomicBoolean(false);
  private XAResourceWrapper wrapper;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    xaActive.set(false);
    wrapper = new XAResourceWrapper(target, xaActive::set);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void start_setsXaActive_andDelegates() throws XAException {
    wrapper.start(xid, XAResource.TMNOFLAGS);
    verify(target).start(xid, XAResource.TMNOFLAGS);
    assertTrue(xaActive.get());
  }

  @Test
  void prepare_doesNotClearFlag() throws XAException {
    wrapper.start(xid, XAResource.TMNOFLAGS);
    when(target.prepare(xid)).thenReturn(XAResource.XA_OK);

    final int rc = wrapper.prepare(xid);

    assertEquals(XAResource.XA_OK, rc);
    verify(target).prepare(xid);
    // The branch is still live between prepare and commit/rollback.
    assertTrue(xaActive.get());
  }

  @Test
  void end_doesNotClearFlag() throws XAException {
    wrapper.start(xid, XAResource.TMNOFLAGS);
    wrapper.end(xid, XAResource.TMSUCCESS);
    verify(target).end(xid, XAResource.TMSUCCESS);
    assertTrue(xaActive.get());
  }

  @Test
  void commit_clearsFlag_andDelegates() throws XAException {
    wrapper.start(xid, XAResource.TMNOFLAGS);
    wrapper.commit(xid, false);
    verify(target).commit(xid, false);
    assertFalse(xaActive.get());
  }

  @Test
  void onePhaseCommit_clearsFlag() throws XAException {
    wrapper.start(xid, XAResource.TMNOFLAGS);
    wrapper.commit(xid, true);
    verify(target).commit(xid, true);
    assertFalse(xaActive.get());
  }

  @Test
  void rollback_clearsFlag_andDelegates() throws XAException {
    wrapper.start(xid, XAResource.TMNOFLAGS);
    wrapper.rollback(xid);
    verify(target).rollback(xid);
    assertFalse(xaActive.get());
  }

  @Test
  void commit_clearsFlagEvenWhenTargetThrows() throws XAException {
    wrapper.start(xid, XAResource.TMNOFLAGS);
    final XAException failure = new XAException(XAException.XAER_RMFAIL);
    org.mockito.Mockito.doThrow(failure).when(target).commit(xid, false);

    try {
      wrapper.commit(xid, false);
    } catch (final XAException expected) {
      // expected
    }
    assertFalse(xaActive.get());
  }

  @Test
  void recover_delegatesUnchanged() throws XAException {
    final Xid[] recovered = new Xid[] {xid};
    when(target.recover(XAResource.TMSTARTRSCAN)).thenReturn(recovered);

    final Xid[] result = wrapper.recover(XAResource.TMSTARTRSCAN);

    assertEquals(recovered, result);
    verify(target).recover(XAResource.TMSTARTRSCAN);
    // recover must never toggle the flag.
    assertFalse(xaActive.get());
  }

  @Test
  void forget_delegates() throws XAException {
    wrapper.forget(xid);
    verify(target).forget(xid);
  }

  @Test
  void transactionTimeout_delegates() throws XAException {
    when(target.getTransactionTimeout()).thenReturn(42);
    when(target.setTransactionTimeout(42)).thenReturn(true);

    assertTrue(wrapper.setTransactionTimeout(42));
    assertEquals(42, wrapper.getTransactionTimeout());
    verify(target).setTransactionTimeout(42);
    verify(target).getTransactionTimeout();
  }

  @Test
  void isSameRM_alwaysFalse_forWrappedResource() throws XAException {
    final XAResource otherTarget = org.mockito.Mockito.mock(XAResource.class);
    final XAResourceWrapper other = new XAResourceWrapper(otherTarget, b -> { });

    // Each wrapped connection is reported as a distinct resource manager so the transaction manager
    // performs a full two-phase commit across separate branches (never a JOIN onto one connection).
    assertFalse(wrapper.isSameRM(other));
    verify(target, never()).isSameRM(org.mockito.ArgumentMatchers.any());
  }

  @Test
  void isSameRM_alwaysFalse_forPlainResource() throws XAException {
    final XAResource plain = org.mockito.Mockito.mock(XAResource.class);

    assertFalse(wrapper.isSameRM(plain));
    verify(target, never()).isSameRM(org.mockito.ArgumentMatchers.any());
  }
}
