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
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostListProvider;
import software.amazon.jdbc.HostListProviderService;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.Messages;

public class ConnectionStringHostListProvider implements StaticHostListProvider {

  final List<HostSpec> hostList = new ArrayList<>();
  Properties properties;
  private boolean isInitialized = false;
  private final ConnectionUrlParser connectionUrlParser;
  private final String initialUrl;
  private final HostListProviderService hostListProviderService;

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

    // TODO: check properties for relevant parameters
    this.initialUrl = initialUrl;
    this.connectionUrlParser = connectionUrlParser;
    this.hostListProviderService = hostListProviderService;
  }

  private void init() throws SQLException {
    if (this.isInitialized) {
      return;
    }
    this.hostList.addAll(this.connectionUrlParser.getHostsFromConnectionUrl(this.initialUrl));
    if (this.hostList.isEmpty()) {
      throw new SQLException(Messages.get("ConnectionStringHostListProvider.parsedListEmpty",
          new Object[]{this.initialUrl}));
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
  public List<HostSpec> refresh(Connection connection) throws SQLException {
    init();
    return this.refresh();
  }

  @Override
  public List<HostSpec> forceRefresh() throws SQLException {
    init();
    return Collections.unmodifiableList(hostList);
  }

  @Override
  public List<HostSpec> forceRefresh(Connection connection) throws SQLException {
    init();
    return this.forceRefresh();
  }
}
