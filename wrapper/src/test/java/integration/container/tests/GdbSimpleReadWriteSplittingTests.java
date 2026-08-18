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

package integration.container.tests;

import integration.TestRegionalClusterInfo;
import integration.container.TestDriverProvider;
import java.util.Properties;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.GlobalAuroraHostListProvider;
import software.amazon.jdbc.plugin.readwritesplitting.SimpleReadWriteSplittingPlugin;

/**
 * The same region rules with topology discovery taken out of the picture.
 *
 * <p>{@code gdbSimpleReadWriteSplitting} is told its two endpoints instead of discovering hosts, which makes it
 * the variant to use when a deployment cannot or will not grant topology queries. It is worth testing separately
 * because the region decisions are then made from the <em>endpoint's</em> region rather than from a topology
 * entry's, and that is a different code path to the same rule.
 *
 * <p>The write endpoint is the primary region's, deliberately. That is what makes the inherited tests mean
 * anything here: the configured writer is out of the home region, which is exactly the situation an application in
 * a secondary region is in, and it is the input the writer restriction and write-forwarding tests act on.
 */
@ExtendWith(TestDriverProvider.class)
@Order(42)
public class GdbSimpleReadWriteSplittingTests extends GdbReadWriteSplittingTests {

  /** The plugin under test, so the auto variant can reuse everything below by changing this one string. */
  protected String pluginCode = "gdbSimpleReadWriteSplitting";

  @Override
  protected Properties getProps() {
    final Properties props = getDefaultProps();
    PropertyDefinition.PLUGINS.set(props, this.pluginCode);

    final TestRegionalClusterInfo home = home();
    final TestRegionalClusterInfo primary = global().getRegion(global().getPrimaryRegion());

    // globalClusterInstanceHostPatterns is still required, even though this variant resolves hosts from the two
    // endpoints below and never runs a topology query. The requirement belongs to the dialect, not to the
    // splitting plugin: naming a global Aurora dialect selects GlobalAuroraHostListProvider, and that provider
    // refuses to initialise without one host template per region regardless of what the rest of the chain does.
    GlobalAuroraHostListProvider.GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.set(
        props, global().getInstanceHostPatterns());

    SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.set(
        props, home.getClusterReadOnlyEndpoint() + ":" + home.getPort());
    SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.set(
        props, primary.getClusterEndpoint() + ":" + primary.getPort());
    return props;
  }
}
