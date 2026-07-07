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

package software.amazon.jdbc.wrapper;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link PreparedStatementRecorder}. */
public class PreparedStatementRecorderTest {

  @Test
  void recordsAndReplaysParametersInOrder() throws SQLException {
    final PreparedStatement target = mock(PreparedStatement.class);
    final PreparedStatementRecorder.Installed<PreparedStatement> installed =
        PreparedStatementRecorder.install(target, PreparedStatement.class);

    installed.proxy.setInt(1, 42);
    installed.proxy.setString(2, "abc");

    assertTrue(installed.recorder.isRebindable());

    final PreparedStatement fresh = mock(PreparedStatement.class);
    installed.recorder.replay(fresh);

    verify(fresh).setInt(1, 42);
    verify(fresh).setString(2, "abc");
  }

  @Test
  void replaysStatementSettingsBeforeParameters() throws SQLException {
    final PreparedStatement target = mock(PreparedStatement.class);
    final PreparedStatementRecorder.Installed<PreparedStatement> installed =
        PreparedStatementRecorder.install(target, PreparedStatement.class);

    installed.proxy.setFetchSize(100);
    installed.proxy.setInt(1, 7);

    final PreparedStatement fresh = mock(PreparedStatement.class);
    installed.recorder.replay(fresh);

    verify(fresh).setFetchSize(100);
    verify(fresh).setInt(1, 7);
  }

  @Test
  void clearParametersClearsParametersButKeepsStatementSettings() throws SQLException {
    final PreparedStatement target = mock(PreparedStatement.class);
    final PreparedStatementRecorder.Installed<PreparedStatement> installed =
        PreparedStatementRecorder.install(target, PreparedStatement.class);

    installed.proxy.setFetchSize(50);
    installed.proxy.setInt(1, 1);
    installed.proxy.clearParameters();

    final PreparedStatement fresh = mock(PreparedStatement.class);
    installed.recorder.replay(fresh);

    verify(fresh).setFetchSize(50);
    verify(fresh, never()).setInt(1, 1);
  }

  @Test
  void streamParameterMakesStatementNotRebindable() throws SQLException {
    final PreparedStatement target = mock(PreparedStatement.class);
    final PreparedStatementRecorder.Installed<PreparedStatement> installed =
        PreparedStatementRecorder.install(target, PreparedStatement.class);

    installed.proxy.setAsciiStream(1, new ByteArrayInputStream(new byte[] {1, 2, 3}));

    assertFalse(installed.recorder.isRebindable());
  }

  @Test
  void pendingBatchMakesStatementNotRebindable() throws SQLException {
    final PreparedStatement target = mock(PreparedStatement.class);
    final PreparedStatementRecorder.Installed<PreparedStatement> installed =
        PreparedStatementRecorder.install(target, PreparedStatement.class);

    installed.proxy.setInt(1, 1);
    installed.proxy.addBatch();

    assertFalse(installed.recorder.isRebindable());
  }

  @Test
  void callableStatement_recordsOutParamRegistrationAndNamedParameters() throws SQLException {
    final CallableStatement target = mock(CallableStatement.class);
    final PreparedStatementRecorder.Installed<CallableStatement> installed =
        PreparedStatementRecorder.install(target, CallableStatement.class);

    installed.proxy.registerOutParameter(1, Types.INTEGER);
    installed.proxy.setString("name", "value");

    assertTrue(installed.recorder.isRebindable());

    final CallableStatement fresh = mock(CallableStatement.class);
    installed.recorder.replay(fresh);

    verify(fresh).registerOutParameter(1, Types.INTEGER);
    verify(fresh).setString("name", "value");
  }

  @Test
  void lastValueWinsForRepeatedParameter() throws SQLException {
    final PreparedStatement target = mock(PreparedStatement.class);
    final PreparedStatementRecorder.Installed<PreparedStatement> installed =
        PreparedStatementRecorder.install(target, PreparedStatement.class);

    installed.proxy.setInt(1, 1);
    installed.proxy.setInt(1, 2);

    final PreparedStatement fresh = mock(PreparedStatement.class);
    installed.recorder.replay(fresh);

    // Both recorded in order; replaying both leaves the last value set on the fresh statement.
    verify(fresh).setInt(1, 1);
    verify(fresh).setInt(1, 2);
  }
}
