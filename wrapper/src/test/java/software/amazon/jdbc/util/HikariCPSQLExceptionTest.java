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

package software.amazon.jdbc.util;

import static org.junit.jupiter.api.Assertions.assertSame;

import com.zaxxer.hikari.SQLExceptionOverride.Override;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.plugin.ConnectionLifetimeFailedSQLException;
import software.amazon.jdbc.plugin.ConnectionLifetimeSQLException;
import software.amazon.jdbc.plugin.ConnectionLifetimeSuccessSQLException;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.plugin.failover.TransactionStateUnknownSQLException;

class HikariCPSQLExceptionTest {

  private final HikariCPSQLException exceptionOverride = new HikariCPSQLException();

  @Test
  void connectionLifetimeSuccessDoesNotEvictConnection() {
    assertSame(
        Override.DO_NOT_EVICT,
        this.exceptionOverride.adjudicate(new ConnectionLifetimeSuccessSQLException()));
  }

  @Test
  void connectionLifetimeFailureContinuesEviction() {
    assertSame(
        Override.CONTINUE_EVICT,
        this.exceptionOverride.adjudicate(
            new ConnectionLifetimeFailedSQLException(new SQLException())));
  }

  @Test
  void connectionLifetimeCloseContinuesEviction() {
    assertSame(
        Override.CONTINUE_EVICT,
        this.exceptionOverride.adjudicate(
            new ConnectionLifetimeSQLException(
                Messages.get("ConnectionLifetimeConnectionPlugin.connectionClosed"),
                SqlState.CONNECTION_NOT_OPEN.getState())));
  }

  @Test
  void failoverSuccessDoesNotEvictConnection() {
    assertSame(
        Override.DO_NOT_EVICT,
        this.exceptionOverride.adjudicate(new FailoverSuccessSQLException()));
  }

  @Test
  void transactionStateUnknownDoesNotEvictConnection() {
    assertSame(
        Override.DO_NOT_EVICT,
        this.exceptionOverride.adjudicate(new TransactionStateUnknownSQLException()));
  }

  @Test
  void ordinarySqlExceptionContinuesEviction() {
    assertSame(
        Override.CONTINUE_EVICT,
        this.exceptionOverride.adjudicate(new SQLException()));
  }
}
