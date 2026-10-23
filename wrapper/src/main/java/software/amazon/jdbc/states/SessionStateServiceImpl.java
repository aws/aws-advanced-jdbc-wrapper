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

package software.amazon.jdbc.states;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.ExecutorFactory;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

public class SessionStateServiceImpl implements SessionStateService {

  private static final Logger LOGGER = Logger.getLogger(SessionStateServiceImpl.class.getName());

  protected SessionState sessionState;
  protected @Nullable SessionState copySessionState;

  /**
   * The original (pristine) session values of each physical connection this logical connection has
   * used, keyed by the physical connection. Only the pristine part of each {@link SessionStateField}
   * is used. A value is recorded the first time it is read from a connection and is never
   * overwritten, so a connection that is switched away from and back to (as read/write splitting
   * does with its cached writer and reader) keeps its true original values and is not queried again.
   * Weak keys let a closed and discarded connection be garbage collected.
   */
  protected final Map<Connection, SessionState> pristineStateByConnection =
      Collections.synchronizedMap(new WeakHashMap<>());

  protected final PluginService pluginService;
  protected final Properties props;

  private boolean authorizationStateTrackingEnabled;

  public SessionStateServiceImpl(
      final @NonNull PluginService pluginService,
      final @NonNull Properties props) {

    this.sessionState = new SessionState();
    this.copySessionState = null;
    this.pluginService = pluginService;
    this.props = props;
  }

  protected boolean transferStateEnabledSetting() {
    return PropertyDefinition.TRANSFER_SESSION_STATE_ON_SWITCH.getBoolean(this.props);
  }

  protected boolean resetStateEnabledSetting() {
    return PropertyDefinition.RESET_SESSION_STATE_ON_CLOSE.getBoolean(this.props);
  }

  @Override
  public Optional<Boolean> getAutoCommit() throws SQLException {
    return this.sessionState.autoCommit.getValue();
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (!this.transferStateEnabledSetting()) {
      return;
    }
    this.sessionState.autoCommit.setValue(autoCommit);
    this.logCurrentState();
  }

  @Override
  public void setupPristineAutoCommit() throws SQLException {
    if (!this.resetStateEnabledSetting()) {
      return;
    }

    if (this.sessionState.autoCommit.getPristineValue().isPresent()) {
      return;
    }
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    this.sessionState.autoCommit.setPristineValue(currentConnection.getAutoCommit());
    this.cachePristineValue(currentConnection, s -> s.autoCommit);
    this.logCurrentState();
  }

  @Override
  public void setupPristineAutoCommit(final boolean autoCommit) throws SQLException {
    if (!this.resetStateEnabledSetting()) {
      return;
    }

    if (this.sessionState.autoCommit.getPristineValue().isPresent()) {
      return;
    }
    this.sessionState.autoCommit.setPristineValue(autoCommit);
    this.cachePristineValue(this.pluginService.getCurrentConnection(), s -> s.autoCommit);
    this.logCurrentState();
  }

  @Override
  public Optional<Boolean> getReadOnly() throws SQLException {
    return this.sessionState.readOnly.getValue();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    if (!this.transferStateEnabledSetting()) {
      return;
    }
    this.sessionState.readOnly.setValue(readOnly);
    this.logCurrentState();
  }

  @Override
  public void setupPristineReadOnly() throws SQLException {
    if (!this.resetStateEnabledSetting()) {
      return;
    }
    if (this.sessionState.readOnly.getPristineValue().isPresent()) {
      return;
    }
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    this.sessionState.readOnly.setPristineValue(currentConnection.isReadOnly());
    this.cachePristineValue(currentConnection, s -> s.readOnly);
    this.logCurrentState();
  }

  @Override
  public void setupPristineReadOnly(final boolean readOnly) throws SQLException {
    if (!this.resetStateEnabledSetting()) {
      return;
    }
    if (this.sessionState.readOnly.getPristineValue().isPresent()) {
      return;
    }
    this.sessionState.readOnly.setPristineValue(readOnly);
    this.cachePristineValue(this.pluginService.getCurrentConnection(), s -> s.readOnly);
    this.logCurrentState();
  }

  @Override
  public Optional<String> getCatalog() throws SQLException {
    return this.sessionState.catalog.getValue();
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    if (!this.transferStateEnabledSetting()) {
      return;
    }
    this.sessionState.catalog.setValue(catalog);
    this.logCurrentState();
  }

  @Override
  public void setupPristineCatalog() throws SQLException {
    if (!this.resetStateEnabledSetting()) {
      return;
    }
    if (this.sessionState.catalog.getPristineValue().isPresent()) {
      return;
    }
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    this.sessionState.catalog.setPristineValue(currentConnection.getCatalog());
    this.cachePristineValue(currentConnection, s -> s.catalog);
    this.logCurrentState();
  }

  @Override
  public void setupPristineCatalog(final String catalog) throws SQLException {
    if (!this.resetStateEnabledSetting()) {
      return;
    }
    if (this.sessionState.catalog.getPristineValue().isPresent()) {
      return;
    }
    this.sessionState.catalog.setPristineValue(catalog);
    this.cachePristineValue(this.pluginService.getCurrentConnection(), s -> s.catalog);
    this.logCurrentState();
  }

  @Override
  public Optional<Integer> getHoldability() throws SQLException {
    return this.sessionState.holdability.getValue();
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    if (!this.transferStateEnabledSetting()) {
      return;
    }
    this.sessionState.holdability.setValue(holdability);
    this.logCurrentState();
  }

  @Override
  public void setupPristineHoldability() throws SQLException {
    if (!this.resetStateEnabledSetting()) {
      return;
    }
    if (this.sessionState.holdability.getPristineValue().isPresent()) {
      return;
    }
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    this.sessionState.holdability.setPristineValue(currentConnection.getHoldability());
    this.cachePristineValue(currentConnection, s -> s.holdability);
    this.logCurrentState();
  }

  @Override
  public void setupPristineHoldability(final int holdability) throws SQLException {
    if (!this.resetStateEnabledSetting()) {
      return;
    }
    if (this.sessionState.holdability.getPristineValue().isPresent()) {
      return;
    }
    this.sessionState.holdability.setPristineValue(holdability);
    this.cachePristineValue(this.pluginService.getCurrentConnection(), s -> s.holdability);
    this.logCurrentState();
  }

  @Override
  public Optional<Integer> getNetworkTimeout() throws SQLException {
    return this.sessionState.networkTimeout.getValue();
  }

  @Override
  public void setNetworkTimeout(int milliseconds) throws SQLException {
    if (!this.transferStateEnabledSetting()) {
      return;
    }
    this.sessionState.networkTimeout.setValue(milliseconds);
    this.logCurrentState();
  }

  @Override
  public void setupPristineNetworkTimeout() throws SQLException {
    if (!this.resetStateEnabledSetting()) {
      return;
    }
    if (this.sessionState.networkTimeout.getPristineValue().isPresent()) {
      return;
    }
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    this.sessionState.networkTimeout.setPristineValue(currentConnection.getNetworkTimeout());
    this.cachePristineValue(currentConnection, s -> s.networkTimeout);
    this.logCurrentState();
  }

  @Override
  public void setupPristineNetworkTimeout(final int milliseconds) throws SQLException {
    if (!this.resetStateEnabledSetting()) {
      return;
    }
    if (this.sessionState.networkTimeout.getPristineValue().isPresent()) {
      return;
    }
    this.sessionState.networkTimeout.setPristineValue(milliseconds);
    this.cachePristineValue(this.pluginService.getCurrentConnection(), s -> s.networkTimeout);
    this.logCurrentState();
  }

  @Override
  public Optional<String> getSchema() throws SQLException {
    return this.sessionState.schema.getValue();
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    if (!this.transferStateEnabledSetting()) {
      return;
    }
    this.sessionState.schema.setValue(schema);
    this.logCurrentState();
  }

  @Override
  public void setupPristineSchema() throws SQLException {
    if (!this.resetStateEnabledSetting()) {
      return;
    }
    if (this.sessionState.schema.getPristineValue().isPresent()) {
      return;
    }
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    this.sessionState.schema.setPristineValue(currentConnection.getSchema());
    this.cachePristineValue(currentConnection, s -> s.schema);
    this.logCurrentState();
  }

  @Override
  public void setupPristineSchema(final String schema) throws SQLException {
    if (!this.resetStateEnabledSetting()) {
      return;
    }
    if (this.sessionState.schema.getPristineValue().isPresent()) {
      return;
    }
    this.sessionState.schema.setPristineValue(schema);
    this.cachePristineValue(this.pluginService.getCurrentConnection(), s -> s.schema);
    this.logCurrentState();
  }

  @Override
  public Optional<Integer> getTransactionIsolation() throws SQLException {
    return this.sessionState.transactionIsolation.getValue();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    if (!this.transferStateEnabledSetting()) {
      return;
    }
    this.sessionState.transactionIsolation.setValue(level);
    this.logCurrentState();
  }

  @Override
  public void setupPristineTransactionIsolation() throws SQLException {
    if (!this.resetStateEnabledSetting()) {
      return;
    }
    if (this.sessionState.transactionIsolation.getPristineValue().isPresent()) {
      return;
    }
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    this.sessionState.transactionIsolation.setPristineValue(currentConnection.getTransactionIsolation());
    this.cachePristineValue(currentConnection, s -> s.transactionIsolation);
    this.logCurrentState();
  }

  @Override
  public void setupPristineTransactionIsolation(final int level) throws SQLException {
    if (!this.resetStateEnabledSetting()) {
      return;
    }
    if (this.sessionState.transactionIsolation.getPristineValue().isPresent()) {
      return;
    }
    this.sessionState.transactionIsolation.setPristineValue(level);
    this.cachePristineValue(this.pluginService.getCurrentConnection(), s -> s.transactionIsolation);
    this.logCurrentState();
  }

  @Override
  public Optional<Map<String, Class<?>>> getTypeMap() throws SQLException {
    return this.sessionState.typeMap.getValue();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    if (!this.transferStateEnabledSetting()) {
      return;
    }
    this.sessionState.typeMap.setValue(map);
    this.logCurrentState();
  }

  @Override
  public void setupPristineTypeMap() throws SQLException {
    if (!this.resetStateEnabledSetting()) {
      return;
    }
    if (this.sessionState.typeMap.getPristineValue().isPresent()) {
      return;
    }
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    this.sessionState.typeMap.setPristineValue(currentConnection.getTypeMap());
    this.cachePristineValue(currentConnection, s -> s.typeMap);
    this.logCurrentState();
  }

  @Override
  public void setupPristineTypeMap(final Map<String, Class<?>> map) throws SQLException {
    if (!this.resetStateEnabledSetting()) {
      return;
    }
    if (this.sessionState.typeMap.getPristineValue().isPresent()) {
      return;
    }
    this.sessionState.typeMap.setPristineValue(map);
    this.cachePristineValue(this.pluginService.getCurrentConnection(), s -> s.typeMap);
    this.logCurrentState();
  }

  @Override
  public Optional<AuthorizationSessionState> getAuthorizationState() {
    return this.sessionState.authorizationState.getValue();
  }

  @Override
  public void enableAuthorizationStateTracking() {
    this.authorizationStateTrackingEnabled = true;
  }

  @Override
  public boolean isAuthorizationStateTrackingEnabled() {
    return this.authorizationStateTrackingEnabled;
  }

  @Override
  public void refreshAuthorizationState() throws SQLException {
    final Optional<AuthorizationSessionState> authorizationState =
        this.pluginService.getTargetDriverDialect().readAuthorizationSessionState(
            this.pluginService.getCurrentConnection());
    if (authorizationState.isPresent()) {
      this.sessionState.authorizationState.setValue(authorizationState.get());
      this.logCurrentState();
    } else {
      this.markAuthorizationStateUnknown();
    }
  }

  @Override
  public void markAuthorizationStateUnknown() {
    this.sessionState.authorizationState.resetValue();
  }

  @Override
  public boolean hasUntrackedAuthorizationState() {
    return this.sessionState.hasUntrackedAuthorizationState;
  }

  @Override
  public void markAuthorizationStateUntracked() {
    this.sessionState.hasUntrackedAuthorizationState = true;
    this.markAuthorizationStateUnknown();
  }

  @Override
  public void reset() {
    this.sessionState.autoCommit.reset();
    this.sessionState.readOnly.reset();
    this.sessionState.catalog.reset();
    this.sessionState.schema.reset();
    this.sessionState.holdability.reset();
    this.sessionState.networkTimeout.reset();
    this.sessionState.transactionIsolation.reset();
    this.sessionState.typeMap.reset();
    this.sessionState.authorizationState.reset();
    this.sessionState.hasUntrackedAuthorizationState = false;
    this.pristineStateByConnection.clear();
  }

  @Override
  public void begin() throws SQLException {
    this.logCurrentState();

    if (!this.transferStateEnabledSetting() && !this.resetStateEnabledSetting()) {
      return;
    }

    if (this.copySessionState != null) {
      throw new SQLException(Messages.get("SessionStateServiceImpl.previousTransferNotCompleted"));
    }

    this.copySessionState = this.sessionState.copy();
  }

  @Override
  public void complete() {
    this.copySessionState = null;
  }

  @Override
  public void applyCurrentSessionState(Connection newConnection) throws SQLException {
    try {
      this.applyCurrentSessionStateInternal(newConnection);
    } finally {
      // SQL-issued authorization state is not transferred by the default implementation because
      // there is no portable JDBC API for restoring it. It must be reacquired from the new physical
      // connection before caching resumes. A custom transfer callback can still inspect the state
      // before it is cleared.
      this.sessionState.authorizationState.resetValue();
    }
  }

  /**
   * Updates the bookkeeping for a switch to {@code newConnection} that does not transfer the session
   * state to it. Nothing is read from or applied to {@code newConnection}, and the custom transfer
   * handler is not called. For each tracked field, the pristine value is replaced by the one recorded
   * for {@code newConnection}, if any, so that close-time restore uses that connection's own original
   * values. The authorization state is reset because it belongs to the previous physical connection.
   *
   * @param newConnection the connection being switched to
   * @throws SQLException declared by {@link SessionStateService}; this implementation does not throw it
   */
  @Override
  public void onSwitchWithoutSessionStateTransfer(final Connection newConnection) throws SQLException {
    try {
      if (!this.transferStateEnabledSetting()) {
        return;
      }

      if (this.sessionState.autoCommit.getValue().isPresent()) {
        this.restoreRecordedPristineValue(newConnection, s -> s.autoCommit);
      }
      if (this.sessionState.readOnly.getValue().isPresent()) {
        this.restoreRecordedPristineValue(newConnection, s -> s.readOnly);
      }
      if (this.sessionState.catalog.getValue().isPresent()) {
        this.restoreRecordedPristineValue(newConnection, s -> s.catalog);
      }
      if (this.sessionState.schema.getValue().isPresent()) {
        this.restoreRecordedPristineValue(newConnection, s -> s.schema);
      }
      if (this.sessionState.holdability.getValue().isPresent()) {
        this.restoreRecordedPristineValue(newConnection, s -> s.holdability);
      }
      if (this.sessionState.transactionIsolation.getValue().isPresent()) {
        this.restoreRecordedPristineValue(newConnection, s -> s.transactionIsolation);
      }
      if (this.sessionState.networkTimeout.getValue().isPresent()) {
        this.restoreRecordedPristineValue(newConnection, s -> s.networkTimeout);
      }
      if (this.sessionState.typeMap.getValue().isPresent()) {
        this.restoreRecordedPristineValue(newConnection, s -> s.typeMap);
      }
      this.logCurrentState();
    } finally {
      this.sessionState.authorizationState.resetValue();
    }
  }

  /**
   * Replaces the pristine value of a session field with the one recorded for {@code connection}, or
   * clears it when none is recorded. Unlike {@link #setupPristineValueForNewConnection}, it never reads
   * from the connection and never records anything.
   *
   * @param connection the physical connection being switched to
   * @param fieldSelector selects the field from a {@link SessionState}
   * @param <T> the field type
   */
  private <T> void restoreRecordedPristineValue(
      final @NonNull Connection connection,
      final @NonNull Function<SessionState, SessionStateField<T>> fieldSelector) {

    final SessionStateField<T> field = fieldSelector.apply(this.sessionState);
    field.resetPristineValue();
    if (!this.resetStateEnabledSetting()) {
      return;
    }
    final SessionState recorded = this.pristineStateByConnection.get(connection);
    if (recorded != null) {
      fieldSelector.apply(recorded).getPristineValue().ifPresent(field::setPristineValue);
    }
  }

  /**
   * Transfers the tracked session state to a new connection. All fields except autoCommit are applied
   * while the connection is in autocommit mode, so that none of them runs inside a transaction. If the
   * connection has autocommit off and a transaction-sensitive field (readOnly, catalog, schema or
   * transaction isolation) is tracked, any open transaction on it is rolled back, and autocommit is
   * turned on, before those fields are applied. The desired autoCommit is applied last, even when
   * applying another field fails.
   *
   * @param newConnection the connection the session state is being transferred to
   * @throws SQLException if a session field cannot be read from or applied to {@code newConnection}
   */
  private void applyCurrentSessionStateInternal(Connection newConnection) throws SQLException {
    if (!this.transferStateEnabledSetting()) {
      return;
    }

    TransferSessionStateOnSwitchCallable callableCopy = Driver.getTransferSessionStateOnSwitchFunc();
    if (callableCopy != null) {
      final boolean isHandled = callableCopy.apply(sessionState, newConnection);
      if (isHandled) {
        // Custom function has handled session transfer
        return;
      }
    }

    final Optional<Boolean> desiredAutoCommit = this.sessionState.autoCommit.getValue();
    final boolean hasTransactionSensitiveState =
        this.sessionState.readOnly.getValue().isPresent()
            || this.sessionState.catalog.getValue().isPresent()
            || this.sessionState.schema.getValue().isPresent()
            || this.sessionState.transactionIsolation.getValue().isPresent();
    final boolean mustReadPristineAutoCommit = desiredAutoCommit.isPresent()
        && this.resetStateEnabledSetting()
        && !this.isPristineValueRecorded(newConnection, s -> s.autoCommit);
    // Read the target's current autoCommit at most once, before any toggling.
    final Boolean targetAutoCommit = (hasTransactionSensitiveState || mustReadPristineAutoCommit)
        ? newConnection.getAutoCommit()
        : null;

    if (desiredAutoCommit.isPresent()) {
      // The pristine autoCommit is captured before the toggle below changes it.
      this.setupPristineValueForNewConnection(newConnection, s -> s.autoCommit,
          () -> targetAutoCommit != null ? targetAutoCommit : newConnection.getAutoCommit());
    }

    final boolean autoCommitTurnedOn = hasTransactionSensitiveState && Boolean.FALSE.equals(targetAutoCommit);
    if (autoCommitTurnedOn) {
      // The target is never the current connection (see PluginServiceImpl). Discard any leftover
      // implicit transaction instead of committing it, so the setters below run outside a transaction.
      newConnection.rollback();
      newConnection.setAutoCommit(true);
    }

    Throwable primaryException = null;
    try {
      if (this.sessionState.readOnly.getValue().isPresent()) {
        this.setupPristineValueForNewConnection(newConnection, s -> s.readOnly, newConnection::isReadOnly);
        newConnection.setReadOnly(this.sessionState.readOnly.getValue().get());
      }

      if (this.sessionState.catalog.getValue().isPresent()) {
        this.setupPristineValueForNewConnection(newConnection, s -> s.catalog, newConnection::getCatalog);
        final String currentCatalog = this.sessionState.catalog.getValue().get();
        if (!StringUtils.isNullOrEmpty(currentCatalog)) {
          newConnection.setCatalog(currentCatalog);
        }
      }

      if (this.sessionState.schema.getValue().isPresent()) {
        this.setupPristineValueForNewConnection(newConnection, s -> s.schema, newConnection::getSchema);
        newConnection.setSchema(this.sessionState.schema.getValue().get());
      }

      if (this.sessionState.holdability.getValue().isPresent()) {
        this.setupPristineValueForNewConnection(newConnection, s -> s.holdability, newConnection::getHoldability);
        newConnection.setHoldability(this.sessionState.holdability.getValue().get());
      }

      if (this.sessionState.transactionIsolation.getValue().isPresent()) {
        this.setupPristineValueForNewConnection(
            newConnection, s -> s.transactionIsolation, newConnection::getTransactionIsolation);
        //noinspection MagicConstant
        newConnection.setTransactionIsolation(this.sessionState.transactionIsolation.getValue().get());
      }

      if (this.sessionState.networkTimeout.getValue().isPresent()) {
        this.setupPristineValueForNewConnection(
            newConnection, s -> s.networkTimeout, newConnection::getNetworkTimeout);
        final ExecutorService executorService =
            ExecutorFactory.newSingleThreadExecutor("session");
        newConnection.setNetworkTimeout(executorService, this.sessionState.networkTimeout.getValue().get());
        executorService.shutdown();
      }

      if (this.sessionState.typeMap.getValue().isPresent()) {
        this.setupPristineValueForNewConnection(newConnection, s -> s.typeMap, newConnection::getTypeMap);
        newConnection.setTypeMap(this.sessionState.typeMap.getValue().get());
      }
    } catch (final Throwable t) {
      primaryException = t;
      throw t;
    } finally {
      this.applyAutoCommitLast(newConnection, desiredAutoCommit, autoCommitTurnedOn, primaryException);
    }
  }

  /**
   * Applies the desired autoCommit to a connection the session state is being transferred to, after
   * every other session field has been applied. When autoCommit is not tracked but was turned on
   * before applying the other fields, the connection's original value ({@code false}) is put back.
   *
   * @param newConnection the connection the session state is being transferred to
   * @param desiredAutoCommit the tracked autoCommit value, if any
   * @param autoCommitTurnedOn whether autoCommit was turned on before applying the other fields
   * @param primaryException the exception thrown while applying the other fields, if any; a failure
   *     here is added to it as a suppressed exception instead of being thrown
   * @throws SQLException if autoCommit cannot be set and there is no {@code primaryException}
   */
  private void applyAutoCommitLast(
      final @NonNull Connection newConnection,
      final @NonNull Optional<Boolean> desiredAutoCommit,
      final boolean autoCommitTurnedOn,
      final @Nullable Throwable primaryException) throws SQLException {

    try {
      if (desiredAutoCommit.isPresent()) {
        // When autoCommit was turned on above and the desired value is true, it is already set.
        if (!(autoCommitTurnedOn && desiredAutoCommit.get())) {
          newConnection.setAutoCommit(desiredAutoCommit.get());
        }
      } else if (autoCommitTurnedOn) {
        newConnection.setAutoCommit(false);
      }
    } catch (final SQLException e) {
      if (primaryException == null) {
        throw e;
      }
      primaryException.addSuppressed(e);
    }
  }

  /**
   * Checks whether a pristine value of a session field is recorded for a physical connection.
   *
   * @param connection the physical connection
   * @param fieldSelector selects the field from a {@link SessionState}
   * @param <T> the field type
   * @return true if a pristine value of the selected field is recorded for {@code connection}
   */
  private <T> boolean isPristineValueRecorded(
      final @NonNull Connection connection,
      final @NonNull Function<SessionState, SessionStateField<T>> fieldSelector) {

    final SessionState recorded = this.pristineStateByConnection.get(connection);
    return recorded != null && fieldSelector.apply(recorded).getPristineValue().isPresent();
  }

  /**
   * Sets up the pristine value of a session field for a connection the session state is being
   * transferred to. The value already recorded for that physical connection is reused when there is
   * one; otherwise it is read from the connection and recorded. Reusing it avoids querying a
   * connection that was already visited (for example, a cached reader or writer during read/write
   * splitting), and keeps its true original value rather than a value the driver applied to it
   * since.
   *
   * @param newConnection the connection the session state is being transferred to
   * @param fieldSelector selects the field from a {@link SessionState}
   * @param pristineValueReader reads the field's current value from {@code newConnection}
   * @param <T> the field type
   * @throws SQLException if the value cannot be read from {@code newConnection}
   */
  private <T> void setupPristineValueForNewConnection(
      final @NonNull Connection newConnection,
      final @NonNull Function<SessionState, SessionStateField<T>> fieldSelector,
      final @NonNull JdbcCallable<T, SQLException> pristineValueReader) throws SQLException {

    final SessionStateField<T> field = fieldSelector.apply(this.sessionState);
    field.resetPristineValue();
    if (!this.resetStateEnabledSetting()) {
      return;
    }

    final SessionStateField<T> cachedField = fieldSelector.apply(
        this.pristineStateByConnection.computeIfAbsent(newConnection, c -> new SessionState()));
    final Optional<T> cachedPristineValue = cachedField.getPristineValue();
    if (cachedPristineValue.isPresent()) {
      field.setPristineValue(cachedPristineValue.get());
    } else {
      field.setPristineValue(pristineValueReader.call());
      field.getPristineValue().ifPresent(cachedField::setPristineValue);
    }
    this.logCurrentState();
  }

  /**
   * Records the pristine value just captured for a session field as the original value of the
   * given physical connection, unless one is already recorded for it.
   *
   * @param connection the physical connection the pristine value was read from
   * @param fieldSelector selects the field from a {@link SessionState}
   * @param <T> the field type
   */
  private <T> void cachePristineValue(
      final @Nullable Connection connection,
      final @NonNull Function<SessionState, SessionStateField<T>> fieldSelector) {

    if (connection == null) {
      return;
    }
    final Optional<T> pristineValue = fieldSelector.apply(this.sessionState).getPristineValue();
    if (!pristineValue.isPresent()) {
      return;
    }
    final SessionStateField<T> cachedField = fieldSelector.apply(
        this.pristineStateByConnection.computeIfAbsent(connection, c -> new SessionState()));
    if (!cachedField.getPristineValue().isPresent()) {
      cachedField.setPristineValue(pristineValue.get());
    }
  }

  /**
   * Returns the session state to restore on the given connection: the given state, with each
   * pristine value replaced by the original value recorded for that physical connection, if any.
   *
   * @param state the session state captured by {@link #begin()}
   * @param connection the connection to restore
   * @return {@code state} itself if nothing is recorded for {@code connection}, otherwise a copy of it
   */
  private SessionState withRecordedPristineValues(
      final @NonNull SessionState state, final @NonNull Connection connection) {

    final SessionState recorded = this.pristineStateByConnection.get(connection);
    if (recorded == null) {
      return state;
    }
    final SessionState result = state.copy();
    overridePristineValue(result.autoCommit, recorded.autoCommit);
    overridePristineValue(result.readOnly, recorded.readOnly);
    overridePristineValue(result.catalog, recorded.catalog);
    overridePristineValue(result.schema, recorded.schema);
    overridePristineValue(result.holdability, recorded.holdability);
    overridePristineValue(result.networkTimeout, recorded.networkTimeout);
    overridePristineValue(result.transactionIsolation, recorded.transactionIsolation);
    overridePristineValue(result.typeMap, recorded.typeMap);
    return result;
  }

  private static <T> void overridePristineValue(
      final @NonNull SessionStateField<T> target, final @NonNull SessionStateField<T> recorded) {
    recorded.getPristineValue().ifPresent(target::setPristineValue);
  }

  @Override
  public void applyPristineSessionState(Connection connection) throws SQLException {
    if (!this.resetStateEnabledSetting()) {
      return;
    }

    ResetSessionStateOnCloseCallable callableCopy = Driver.getResetSessionStateOnCloseFunc();
    if (callableCopy != null) {
      final boolean isHandled = callableCopy.apply(sessionState, connection);
      if (isHandled) {
        // Custom function has handled session transfer
        return;
      }
    }

    final SessionState capturedSessionState = this.copySessionState;
    if (capturedSessionState == null) {
      return;
    }
    final SessionState copySessionState = this.withRecordedPristineValues(capturedSessionState, connection);

    final boolean restoreAutoCommit = copySessionState.autoCommit.canRestorePristine();
    //noinspection OptionalGetWithoutIsPresent
    final boolean pristineAutoCommit = restoreAutoCommit && copySessionState.autoCommit.getPristineValue().get();
    final boolean restoreTransactionSensitiveState =
        copySessionState.readOnly.canRestorePristine()
            || copySessionState.catalog.canRestorePristine()
            || copySessionState.schema.canRestorePristine()
            || copySessionState.transactionIsolation.canRestorePristine();

    boolean autoCommitTurnedOn = false;
    boolean rollbackFailed = false;
    if (restoreTransactionSensitiveState || pristineAutoCommit) {
      boolean currentAutoCommit = true;
      try {
        currentAutoCommit = connection.getAutoCommit();
      } catch (final SQLException e) {
        // Ignore any exception and restore without turning autocommit on first.
      }
      if (!currentAutoCommit) {
        try {
          // Discard uncommitted work instead of letting setAutoCommit(true) commit it.
          connection.rollback();
          try {
            connection.setAutoCommit(true);
            autoCommitTurnedOn = true;
          } catch (final SQLException e) {
            // Ignore any exception
          }
        } catch (final SQLException e) {
          // Never turn autocommit on here: it would commit the open transaction.
          rollbackFailed = true;
        }
      }
    }

    try {
      this.restorePristineFieldsExceptAutoCommit(connection, copySessionState);
    } finally {
      if (restoreAutoCommit && !(rollbackFailed && pristineAutoCommit)
          && !(autoCommitTurnedOn && pristineAutoCommit)) {
        try {
          connection.setAutoCommit(pristineAutoCommit);
        } catch (final SQLException e) {
          // Ignore any exception
        }
      } else if (!restoreAutoCommit && autoCommitTurnedOn) {
        try {
          connection.setAutoCommit(false);
        } catch (final SQLException e) {
          // Ignore any exception
        }
      }
    }
  }

  /**
   * Restores the pristine value of every restorable session field except autoCommit, ignoring any
   * exception.
   *
   * @param connection the connection to restore
   * @param copySessionState the session state holding the pristine values to restore
   */
  private void restorePristineFieldsExceptAutoCommit(
      final @NonNull Connection connection, final @NonNull SessionState copySessionState) {

    if (copySessionState.readOnly.canRestorePristine()) {
      try {
        //noinspection OptionalGetWithoutIsPresent
        connection.setReadOnly(copySessionState.readOnly.getPristineValue().get());
      } catch (final SQLException e) {
        // Ignore any exception
      }
    }

    if (copySessionState.catalog.canRestorePristine()) {
      try {
        //noinspection OptionalGetWithoutIsPresent
        final String pristineCatalog = copySessionState.catalog.getPristineValue().get();
        if (!StringUtils.isNullOrEmpty(pristineCatalog)) {
          connection.setCatalog(pristineCatalog);
        }
      } catch (final SQLException e) {
        // Ignore any exception
      }
    }

    if (copySessionState.schema.canRestorePristine()) {
      try {
        //noinspection OptionalGetWithoutIsPresent
        connection.setSchema(copySessionState.schema.getPristineValue().get());
      } catch (final SQLException e) {
        // Ignore any exception
      }
    }

    if (copySessionState.holdability.canRestorePristine()) {
      try {
        //noinspection OptionalGetWithoutIsPresent
        connection.setHoldability(copySessionState.holdability.getPristineValue().get());
      } catch (final SQLException e) {
        // Ignore any exception
      }
    }

    if (copySessionState.transactionIsolation.canRestorePristine()) {
      try {
        //noinspection OptionalGetWithoutIsPresent,MagicConstant
        connection.setTransactionIsolation(
            copySessionState.transactionIsolation.getPristineValue().get());
      } catch (final SQLException e) {
        // Ignore any exception
      }
    }

    if (copySessionState.networkTimeout.canRestorePristine()) {
      try {
        final ExecutorService executorService =
            ExecutorFactory.newSingleThreadExecutor("session");
        //noinspection OptionalGetWithoutIsPresent
        connection.setNetworkTimeout(executorService,
            copySessionState.networkTimeout.getPristineValue().get());
        executorService.shutdown();
      } catch (final SQLException e) {
        // Ignore any exception
      }
    }

    if (copySessionState.typeMap.canRestorePristine()) {
      try {
        //noinspection OptionalGetWithoutIsPresent
        connection.setTypeMap(copySessionState.typeMap.getPristineValue().get());
      } catch (final SQLException e) {
        // Ignore any exception
      }
    }
  }

  public void logCurrentState() {
    LOGGER.finest(() -> Messages.get(
        "SessionStateServiceImpl.currentSessionState", new Object[] {this.sessionState}));
  }
}
