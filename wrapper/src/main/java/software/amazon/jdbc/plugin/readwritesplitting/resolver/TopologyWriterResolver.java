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

package software.amazon.jdbc.plugin.readwritesplitting.resolver;

import java.sql.Connection;
import java.sql.SQLException;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.util.Messages;

/**
 * {@link WriterResolver} that connects to the writer identified from the cluster topology (set by
 * the {@link TopologyRefresher} before this runs). Ports the legacy
 * {@code ReadWriteSplittingPlugin.initializeWriterConnection} connect step.
 */
public class TopologyWriterResolver implements WriterResolver {

  @Override
  public WriterResolution resolveWriter(final RwSplitContext ctx) throws SQLException {
    final HostSpec writerHost = ctx.writerHostSpec();
    if (writerHost == null) {
      ctx.logAndThrow(Messages.get("ReadWriteSplittingPlugin.noWriterFound"));
      return WriterResolution.stay();
    }
    final Connection conn = ctx.connect(writerHost, ctx.properties());
    return WriterResolution.connected(conn, writerHost);
  }
}
