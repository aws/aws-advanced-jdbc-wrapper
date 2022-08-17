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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import software.amazon.jdbc.HostListProvider;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.util.ConnectionUrlParser;

public class ConnectionStringHostListProvider implements HostListProvider, StaticHostListProvider {

  final List<HostSpec> hostList = new ArrayList<>();
  Properties properties;

  public ConnectionStringHostListProvider(final Properties properties, final String initialUrl) {
    this(properties, initialUrl, new ConnectionUrlParser());
  }

  ConnectionStringHostListProvider(
      final Properties properties,
      final String initialUrl,
      final ConnectionUrlParser connectionUrlParser) {
    // TODO: check properties for relevant parameters

    hostList.addAll(connectionUrlParser.getHostsFromConnectionUrl(initialUrl));
  }

  @Override
  public List<HostSpec> refresh() throws SQLException {
    return Collections.unmodifiableList(hostList);
  }

  @Override
  public List<HostSpec> forceRefresh() throws SQLException {
    return Collections.unmodifiableList(hostList);
  }
}
