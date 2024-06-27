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
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.StringUtils;

public class SessionStateServiceImpl implements SessionStateService {

  private static final Logger LOGGER = Logger.getLogger(SessionStateServiceImpl.class.getName());

  protected SessionState sessionState;
  protected SessionState copySessionState;

  protected final PluginService pluginService;
  protected final Properties props;


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
    this.sessionState.autoCommit.setPristineValue(this.pluginService.getCurrentConnection().getAutoCommit());
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
    this.sessionState.readOnly.setPristineValue(this.pluginService.getCurrentConnection().isReadOnly());
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
    this.sessionState.catalog.setPristineValue(this.pluginService.getCurrentConnection().getCatalog());
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
    this.sessionState.holdability.setPristineValue(this.pluginService.getCurrentConnection().getHoldability());
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
    this.sessionState.networkTimeout.setPristineValue(this.pluginService.getCurrentConnection().getNetworkTimeout());
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
    this.sessionState.schema.setPristineValue(this.pluginService.getCurrentConnection().getSchema());
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
    this.sessionState.transactionIsolation.setPristineValue(
        this.pluginService.getCurrentConnection().getTransactionIsolation());
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
    this.sessionState.typeMap.setPristineValue(this.pluginService.getCurrentConnection().getTypeMap());
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
    this.logCurrentState();
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
  }

  @Override
  public void begin() throws SQLException {
    this.logCurrentState();

    if (!this.transferStateEnabledSetting() && !this.resetStateEnabledSetting()) {
      return;
    }

    if (this.copySessionState != null) {
      throw new SQLException("Previous session state transfer is not completed.");
    }

    this.copySessionState = this.sessionState.copy();
  }

  @Override
  public void complete() {
    this.copySessionState = null;
  }

  @Override
  public void applyCurrentSessionState(Connection newConnection) throws SQLException {
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

    if (this.sessionState.autoCommit.getValue().isPresent()) {
      this.sessionState.autoCommit.resetPristineValue();
      this.setupPristineAutoCommit();
      newConnection.setAutoCommit(this.sessionState.autoCommit.getValue().get());
    }

    if (this.sessionState.readOnly.getValue().isPresent()) {
      this.sessionState.readOnly.resetPristineValue();
      this.setupPristineReadOnly();
      newConnection.setReadOnly(this.sessionState.readOnly.getValue().get());
    }

    if (this.sessionState.catalog.getValue().isPresent()) {
      this.sessionState.catalog.resetPristineValue();
      this.setupPristineCatalog();
      final String currentCatalog = this.sessionState.catalog.getValue().get();
      if (!StringUtils.isNullOrEmpty(currentCatalog)) {
        newConnection.setCatalog(currentCatalog);
      }
    }

    if (this.sessionState.schema.getValue().isPresent()) {
      this.sessionState.schema.resetPristineValue();
      this.setupPristineSchema();
      newConnection.setSchema(this.sessionState.schema.getValue().get());
    }

    if (this.sessionState.holdability.getValue().isPresent()) {
      this.sessionState.holdability.resetPristineValue();
      this.setupPristineHoldability();
      newConnection.setHoldability(this.sessionState.holdability.getValue().get());
    }

    if (this.sessionState.transactionIsolation.getValue().isPresent()) {
      this.sessionState.transactionIsolation.resetPristineValue();
      this.setupPristineTransactionIsolation();
      //noinspection MagicConstant
      newConnection.setTransactionIsolation(this.sessionState.transactionIsolation.getValue().get());
    }

    if (this.sessionState.networkTimeout.getValue().isPresent()) {
      this.sessionState.networkTimeout.resetPristineValue();
      this.setupPristineNetworkTimeout();
      final ExecutorService executorService = Executors.newSingleThreadExecutor();
      newConnection.setNetworkTimeout(executorService, this.sessionState.networkTimeout.getValue().get());
      executorService.shutdown();
    }

    if (this.sessionState.typeMap.getValue().isPresent()) {
      this.sessionState.typeMap.resetPristineValue();
      this.setupPristineTypeMap();
      newConnection.setTypeMap(this.sessionState.typeMap.getValue().get());
    }
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

    if (this.copySessionState.autoCommit.canRestorePristine()) {
      try {
        //noinspection OptionalGetWithoutIsPresent
        connection.setAutoCommit(this.copySessionState.autoCommit.getPristineValue().get());
      } catch (final SQLException e) {
        // Ignore any exception
      }
    }

    if (this.copySessionState.readOnly.canRestorePristine()) {
      try {
        //noinspection OptionalGetWithoutIsPresent
        connection.setReadOnly(this.copySessionState.readOnly.getPristineValue().get());
      } catch (final SQLException e) {
        // Ignore any exception
      }
    }

    if (this.copySessionState.catalog.canRestorePristine()) {
      try {
        //noinspection OptionalGetWithoutIsPresent
        final String pristineCatalog = this.copySessionState.catalog.getPristineValue().get();
        if (!StringUtils.isNullOrEmpty(pristineCatalog)) {
          connection.setCatalog(pristineCatalog);
        }
      } catch (final SQLException e) {
        // Ignore any exception
      }
    }

    if (this.copySessionState.schema.canRestorePristine()) {
      try {
        //noinspection OptionalGetWithoutIsPresent
        connection.setSchema(this.copySessionState.schema.getPristineValue().get());
      } catch (final SQLException e) {
        // Ignore any exception
      }
    }

    if (this.copySessionState.holdability.canRestorePristine()) {
      try {
        //noinspection OptionalGetWithoutIsPresent
        connection.setHoldability(this.copySessionState.holdability.getPristineValue().get());
      } catch (final SQLException e) {
        // Ignore any exception
      }
    }

    if (this.copySessionState.transactionIsolation.canRestorePristine()) {
      try {
        //noinspection OptionalGetWithoutIsPresent,MagicConstant
        connection.setTransactionIsolation(
            this.copySessionState.transactionIsolation.getPristineValue().get());
      } catch (final SQLException e) {
        // Ignore any exception
      }
    }

    if (this.copySessionState.networkTimeout.canRestorePristine()) {
      try {
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        //noinspection OptionalGetWithoutIsPresent
        connection.setNetworkTimeout(executorService,
            this.copySessionState.networkTimeout.getPristineValue().get());
        executorService.shutdown();
      } catch (final SQLException e) {
        // Ignore any exception
      }
    }

    if (this.copySessionState.typeMap.canRestorePristine()) {
      try {
        //noinspection OptionalGetWithoutIsPresent
        connection.setTypeMap(this.copySessionState.typeMap.getPristineValue().get());
      } catch (final SQLException e) {
        // Ignore any exception
      }
    }
  }

  public void logCurrentState() {
    LOGGER.finest(() -> "Current session state:\n" + this.sessionState);
  }
}
