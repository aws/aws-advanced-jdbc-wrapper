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

package software.amazon.jdbc.plugin.dev;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.connection.ConnectConfig;

public class DeveloperConnectionPlugin extends AbstractConnectionPlugin implements ExceptionSimulator {

  private static final Logger LOGGER =
      Logger.getLogger(DeveloperConnectionPlugin.class.getName());

  private static final String ALL_METHODS = "*";

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<>(Collections.singletonList(ALL_METHODS)));

  private String nextMethodName;
  private Throwable nextException;
  private ExceptionSimulatorExecuteJdbcMethodCallback exceptionSimulatorExecuteJdbcMethodCallback;

  public DeveloperConnectionPlugin() { }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public void raiseExceptionOnNextCall(final Throwable throwable) {
    this.nextException = throwable;
    this.nextMethodName = ALL_METHODS;
  }

  @Override
  public void raiseExceptionOnNextCall(final @NonNull String methodName, final Throwable throwable) {
    if (StringUtils.isNullOrEmpty(methodName)) {
      throw new RuntimeException("methodName should not be empty.");
    }
    this.nextException = throwable;
    this.nextMethodName = methodName;
  }

  @Override
  public void setCallback(
      final ExceptionSimulatorExecuteJdbcMethodCallback exceptionSimulatorExecuteJdbcMethodCallback) {
    this.exceptionSimulatorExecuteJdbcMethodCallback = exceptionSimulatorExecuteJdbcMethodCallback;
  }

  @Override
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs)
      throws E {

    this.raiseExceptionIfNeeded(
        resultClass,
        exceptionClass,
        methodName,
        jdbcMethodArgs);

    return jdbcMethodFunc.call();
  }

  protected <T, E extends Exception> void raiseExceptionIfNeeded(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final String methodName,
      final Object[] jdbcMethodArgs)
      throws E {

    if (this.nextException != null) {
      if (ALL_METHODS.equals(this.nextMethodName) || methodName.equals(this.nextMethodName)) {
        this.raiseException(exceptionClass, this.nextException, methodName);
      }
    } else if (this.exceptionSimulatorExecuteJdbcMethodCallback != null) {
      this.raiseException(
          exceptionClass,
          this.exceptionSimulatorExecuteJdbcMethodCallback.getExceptionToRaise(
              resultClass,
              exceptionClass,
              methodName,
              jdbcMethodArgs),
          methodName);
    }
  }

  protected <E extends Exception> void raiseException(
      final Class<E> exceptionClass,
      final Throwable throwable,
      final String methodName)
      throws E {

    if (throwable == null) {
      return;
    }

    if (throwable instanceof RuntimeException) {
      this.nextException = null;
      this.nextMethodName = null;
      LOGGER.finest(() -> String.format("Raise an exception %s while executing %s.",
          throwable.getClass().getName(), methodName));
      throw (RuntimeException) throwable;
    } else {
      E resulException = WrapperUtils.wrapExceptionIfNeeded(exceptionClass, throwable);
      this.nextException = null;
      this.nextMethodName = null;
      LOGGER.finest(() -> String.format("Raise an exception %s while executing %s.",
          resulException.getClass().getName(), methodName));
      throw resulException;
    }
  }

  @Override
  public Connection connect(
      final ConnectConfig connectConfig,
      final HostSpec hostSpec,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {
    this.raiseExceptionOnConnectIfNeeded(connectConfig, hostSpec, isInitialConnection);
    return super.connect(connectConfig, hostSpec, isInitialConnection, connectFunc);
  }

  @Override
  public Connection forceConnect(
      final ConnectConfig connectConfig,
      final HostSpec hostSpec,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {

    this.raiseExceptionOnConnectIfNeeded(connectConfig, hostSpec, isInitialConnection);
    return super.connect(connectConfig, hostSpec, isInitialConnection, forceConnectFunc);
  }

  protected void raiseExceptionOnConnectIfNeeded(
      final ConnectConfig connectConfig,
      final HostSpec hostSpec,
      final boolean isInitialConnection)
      throws SQLException {

    if (ExceptionSimulatorManager.nextException != null) {
      this.raiseExceptionOnConnect(ExceptionSimulatorManager.nextException);
    } else if (ExceptionSimulatorManager.connectCallback != null) {
      this.raiseExceptionOnConnect(
          ExceptionSimulatorManager.connectCallback.getExceptionToRaise(
              connectConfig, hostSpec, isInitialConnection));
    }
  }

  protected void raiseExceptionOnConnect(final Throwable throwable)
      throws SQLException {

    if (throwable == null) {
      return;
    }

    if (throwable instanceof RuntimeException) {
      ExceptionSimulatorManager.nextException = null;
      LOGGER.finest(() -> String.format("Raise an exception %s while opening a new connection.",
          ExceptionSimulatorManager.nextException.getClass().getName()));
      throw (RuntimeException) throwable;
    } else {
      SQLException resulException = WrapperUtils.wrapExceptionIfNeeded(SQLException.class, throwable);
      ExceptionSimulatorManager.nextException = null;
      LOGGER.finest(() -> String.format("Raise an exception %s while opening a new connection.",
          resulException.getClass().getName()));
      throw resulException;
    }
  }
}
