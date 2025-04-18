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

package software.amazon.jdbc.util.connection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.ServiceContainer;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

public class ConnectionServiceImpl implements ConnectionService {
  protected ServiceContainer serviceContainer;
  protected ConnectionProvider connectionProvider;
  protected TargetDriverDialect driverDialect;

  public ConnectionServiceImpl(
      ServiceContainer serviceContainer, ConnectionProvider connectionProvider, TargetDriverDialect driverDialect) {
    this.serviceContainer = serviceContainer;
    this.connectionProvider = connectionProvider;
    this.driverDialect = driverDialect;
  }

  @Override
  public Connection createAuxiliaryConnection(String underlyingDriverConnString, Properties props) throws SQLException {
    return new ConnectionWrapper(
        this.serviceContainer,
        props,
        underlyingDriverConnString,
        this.connectionProvider,
        null,
        this.driverDialect, null
    );
  }
}
