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

package software.amazon.jdbc.benchmarks.support;

import java.sql.Connection;
import java.util.List;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.hostlistprovider.HostListProvider;
import software.amazon.jdbc.hostlistprovider.StaticHostListProvider;

/**
 * A {@link HostListProvider} that returns a fixed topology without querying anything.
 *
 * <p>Used to populate {@code PluginServiceImpl}'s host list through its real
 * {@code refreshHostList()} path, rather than reaching into the field. That keeps the setup faithful
 * - {@code setNodeList} runs and computes node-change notifications exactly as it would in
 * production - while requiring no database.
 *
 * <p>Implements {@link StaticHostListProvider} so {@code isStaticHostListProvider()} reports true,
 * matching a provider built from a fixed URL host list.
 */
public class StaticTopologyProvider implements HostListProvider, StaticHostListProvider {

  private final List<HostSpec> hosts;

  public StaticTopologyProvider(final List<HostSpec> hosts) {
    this.hosts = hosts;
  }

  @Override
  public List<HostSpec> getCurrentTopology(final Connection conn, final HostSpec initialHostSpec) {
    return this.hosts;
  }

  @Override
  public List<HostSpec> refresh() {
    return this.hosts;
  }

  @Override
  public List<HostSpec> forceRefresh() {
    return this.hosts;
  }

  @Override
  public List<HostSpec> forceRefresh(final boolean verifyTopology, final long timeoutMs) {
    return this.hosts;
  }

  @Override
  public String getClusterId() {
    return "benchmark-cluster";
  }

  @Override
  public void stopMonitor() {
    // no monitor to stop
  }
}
