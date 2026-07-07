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

package software.amazon.jdbc.plugin.readwritesplitting;

import java.util.Properties;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.readwritesplitting.gate.TransactionAwareGate;
import software.amazon.jdbc.plugin.readwritesplitting.signal.CompositeSignal;
import software.amazon.jdbc.plugin.readwritesplitting.signal.ReadOnlyFlagSignal;
import software.amazon.jdbc.plugin.readwritesplitting.signal.SqlRoutingSignal;

/**
 * SQL-routed, endpoint-based Global Database read/write splitting plugin
 * ({@code gdbAutoSimpleReadWriteSplitting}): combines SQL-driven routing (at statement-creation
 * time) with configured write/read endpoints and the Global Database region rules / Global Write
 * Forwarding. Requires the {@code sqlParser} plugin to be ordered before it.
 */
public class GdbAutoSimpleReadWriteSplittingPlugin extends GdbSimpleReadWriteSplittingPlugin {

  public GdbAutoSimpleReadWriteSplittingPlugin(final PluginService pluginService, final Properties properties) {
    super(pluginService, properties, gdbAutoSimple(properties));
  }

  private static RwSplitHelpers gdbAutoSimple(final Properties props) {
    return gdbEndpointHelpers(props,
        new CompositeSignal(new SqlRoutingSignal(), new ReadOnlyFlagSignal()),
        new TransactionAwareGate(true, true));
  }
}
