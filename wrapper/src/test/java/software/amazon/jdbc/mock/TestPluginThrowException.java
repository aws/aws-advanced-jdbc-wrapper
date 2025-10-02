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

package software.amazon.jdbc.mock;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.util.connection.ConnectConfig;

public class TestPluginThrowException extends TestPluginOne {

  protected final Class<? extends Exception> exceptionClass;
  protected final boolean isBefore;

  public TestPluginThrowException(
      ArrayList<String> calls, Class<? extends Exception> exceptionClass, boolean isBefore) {
    super();
    this.calls = calls;
    this.exceptionClass = exceptionClass;
    this.isBefore = isBefore;

    this.subscribedMethods = new HashSet<>(Arrays.asList("*"));
  }

  @Override
  public <T, E extends Exception> T execute(
      Class<T> resultClass,
      Class<E> exceptionClass,
      Object methodInvokeOn,
      String methodName,
      JdbcCallable<T, E> jdbcMethodFunc,
      Object[] jdbcMethodArgs)
      throws E {

    this.calls.add(this.getClass().getSimpleName() + ":before");
    if (this.isBefore) {
      try {
        throw this.exceptionClass.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    T result = jdbcMethodFunc.call();

    this.calls.add(this.getClass().getSimpleName() + ":after");
    //noinspection ConstantConditions
    if (!this.isBefore) {
      try {
        throw this.exceptionClass.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return result;
  }

  @Override
  public Connection connect(
      final ConnectConfig connectConfig,
      final HostSpec hostSpec,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {

    this.calls.add(this.getClass().getSimpleName() + ":before");
    if (this.isBefore) {
      throwException();
    }

    Connection conn = connectFunc.call();

    this.calls.add(this.getClass().getSimpleName() + ":after");
    if (!this.isBefore) {
      throwException();
    }

    return conn;
  }

  private void throwException() throws SQLException {
    try {
      throw this.exceptionClass.newInstance();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      if (e instanceof SQLException) {
        throw (SQLException) e;
      }
      throw new SQLException(e);
    }
  }
}
