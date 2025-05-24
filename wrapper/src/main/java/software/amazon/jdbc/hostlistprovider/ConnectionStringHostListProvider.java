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

package software.amazon.jdbc.hostlistprovider;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.Messages;

public class ConnectionStringHostListProvider implements StaticHostListProvider {

  private static final Logger LOGGER = Logger.getLogger(ConnectionStringHostListProvider.class.getName());

  final List<HostSpec> hostList = new ArrayList<>();
  Properties properties;
  private boolean isInitialized = false;
  private final boolean isSingleWriterConnectionString;
  private final ConnectionUrlParser connectionUrlParser;
  private final String initialUrl;
  private final HostListProviderService hostListProviderService;

  public static final AwsWrapperProperty SINGLE_WRITER_CONNECTION_STRING =
      new AwsWrapperProperty(
          "singleWriterConnectionString",
          "false",
          "Set to true if you are providing a connection string with multiple comma-delimited hosts and your "
              + "cluster has only one writer. The writer must be the first host in the connection string");

  public ConnectionStringHostListProvider(
      final @NonNull Properties properties,
      final String initialUrl,
      final @NonNull HostListProviderService hostListProviderService) {
    this(properties, initialUrl, hostListProviderService, new ConnectionUrlParser());
  }

  ConnectionStringHostListProvider(
      final @NonNull Properties properties,
      final String initialUrl,
      final @NonNull HostListProviderService hostListProviderService,
      final @NonNull ConnectionUrlParser connectionUrlParser) {

    this.isSingleWriterConnectionString = SINGLE_WRITER_CONNECTION_STRING.getBoolean(properties);
    this.initialUrl = initialUrl;
    this.connectionUrlParser = connectionUrlParser;
    this.hostListProviderService = hostListProviderService;
  }

  private void init() throws SQLException {
    if (this.isInitialized) {
      return;
    }
    this.hostList.addAll(
        this.connectionUrlParser.getHostsFromConnectionUrl(this.initialUrl, this.isSingleWriterConnectionString,
            () -> this.hostListProviderService.getHostSpecBuilder()));
    if (this.hostList.isEmpty()) {
      throw new SQLException(Messages.get("ConnectionStringHostListProvider.parsedListEmpty",
          new Object[] {this.initialUrl}));
    }
    this.hostListProviderService.setInitialConnectionHostSpec(this.hostList.get(0));
    this.isInitialized = true;
  }

  @Override
  public List<HostSpec> refresh() throws SQLException {
    init();
    return Collections.unmodifiableList(hostList);
  }

  @Override
  public List<HostSpec> refresh(final Connection connection) throws SQLException {
    init();
    return this.refresh();
  }

  @Override
  public List<HostSpec> forceRefresh() throws SQLException {
    init();
    return Collections.unmodifiableList(hostList);
  }

  @Override
  public List<HostSpec> forceRefresh(final Connection connection) throws SQLException {
    init();
    return this.forceRefresh();
  }

  @Override
  public HostRole getHostRole(Connection connection) {
    throw new UnsupportedOperationException("ConnectionStringHostListProvider does not support getHostRole");
  }

  @Override
  public HostSpec identifyConnection(Connection connection) throws SQLException {
    LOGGER.finest(Messages.get("ConnectionStringHostListProvider.unsupportedIdentifyConnection"));
    return null;
  }

  @Override
  public String getClusterId() throws UnsupportedOperationException {
    return "<none>";
  }
}
