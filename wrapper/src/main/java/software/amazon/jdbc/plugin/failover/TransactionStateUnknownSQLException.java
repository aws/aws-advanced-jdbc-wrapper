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

package software.aws.jdbc.plugin.failover;

import software.aws.jdbc.util.Messages;
import software.aws.jdbc.util.SqlState;

public class TransactionStateUnknownSQLException extends FailoverSQLException {

  public TransactionStateUnknownSQLException(Throwable cause) {
    super(Messages.get("Failover.transactionResolutionUnknownError"),
        SqlState.CONNECTION_FAILURE_DURING_TRANSACTION.getState(), cause);
  }

  public TransactionStateUnknownSQLException(String message) {
    super(message, SqlState.CONNECTION_FAILURE_DURING_TRANSACTION.getState());
  }

  public TransactionStateUnknownSQLException() {
    super(Messages.get("Failover.transactionResolutionUnknownError"),
        SqlState.CONNECTION_FAILURE_DURING_TRANSACTION.getState());
  }
}
