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

package software.amazon.jdbc.plugin.readwritesplitting.refresher;

import java.sql.SQLException;
import java.util.List;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Utils;

/**
 * Topology-aware {@link TopologyRefresher}: refreshes the host list and records the writer host,
 * reproducing the legacy {@code ReadWriteSplittingPlugin.refreshAndStoreTopology}. The refresh no
 * longer depends on the current connection (the host list provider does not use it), so it is
 * always attempted.
 */
public class TopologyRefresherImpl implements TopologyRefresher {

  @Override
  public void refresh(final RwSplitContext ctx) throws SQLException {
    try {
      ctx.pluginService().refreshHostList();
    } catch (final SQLException e) {
      // ignore
    }

    final List<HostSpec> hosts = ctx.pluginService().getHosts();
    if (Utils.isNullOrEmpty(hosts)) {
      ctx.logAndThrow(Messages.get("ReadWriteSplittingPlugin.emptyHostList"));
      return;
    }

    final HostSpec writerHost = Utils.getWriter(hosts);
    if (writerHost == null) {
      ctx.logAndThrow(Messages.get("ReadWriteSplittingPlugin.noWriterFound"));
      return;
    }
    ctx.setWriterHostSpec(writerHost);
  }
}
