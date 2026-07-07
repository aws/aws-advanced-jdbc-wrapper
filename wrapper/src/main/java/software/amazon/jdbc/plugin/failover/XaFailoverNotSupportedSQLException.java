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

package software.amazon.jdbc.plugin.failover;

import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;

/**
 * Thrown when failover is triggered while the connection is enlisted in an active XA transaction
 * branch. The broken connection cannot be retained and the branch cannot be preserved across a
 * node change, so the driver fails fast (rather than swapping the physical connection) and lets the
 * transaction manager roll the branch back.
 *
 * <p>Extends {@link FailoverSQLException} so existing {@code catch (FailoverSQLException)} handling
 * (e.g. read/write splitting idle-connection cleanup) continues to apply. Uses SQL state
 * {@code 08007} (connection failure during transaction), which a transaction manager interprets as
 * a failed branch.
 */
public class XaFailoverNotSupportedSQLException extends FailoverSQLException {

  public XaFailoverNotSupportedSQLException() {
    super(Messages.get("Failover.failoverNotSupportedDuringXaTransaction"),
        SqlState.CONNECTION_FAILURE_DURING_TRANSACTION.getState());
  }
}
