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

package software.amazon.jdbc.plugin.limitless;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;

public class LimitlessConnectionContext {
  private HostSpec hostSpec;
  private Properties props;
  private Properties origProps;
  private Connection connection;
  private JdbcCallable<Connection, SQLException> connectFunc;
  private List<HostSpec> limitlessRouters;

  public LimitlessConnectionContext(
      final HostSpec hostSpec,
      final Properties props,
      final Properties origProps,
      final Connection connection,
      final JdbcCallable<Connection, SQLException> connectFunc,
      final List<HostSpec> limitlessRouters
  ) {
    this.hostSpec = hostSpec;
    this.props = props;
    this.origProps = origProps;
    this.connection = connection;
    this.connectFunc = connectFunc;
    this.limitlessRouters = limitlessRouters;
  }

  public HostSpec getHostSpec() {
    return this.hostSpec;
  }

  public Properties getProps() {
    return this.props;
  }

  public Properties getOrigProps() {
    return this.origProps;
  }

  public Connection getConnection() {
    return this.connection;
  }

  public void setConnection(final @NonNull Connection connection) {
    this.connection = connection;
  }

  public JdbcCallable<Connection, SQLException> getConnectFunc() {
    return this.connectFunc;
  }

  public List<HostSpec> getLimitlessRouters() {
    return this.limitlessRouters;
  }

  public void setLimitlessRouters(final @NonNull List<HostSpec> limitlessRouters) {
    this.limitlessRouters = limitlessRouters;
  }
}
