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

import integration.container.TestDriverProvider;
import java.util.Properties;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostlistprovider.GlobalAuroraHostListProvider;

/**
 * The same region rules, reached by inspecting SQL instead of by {@code setReadOnly}.
 *
 * <p>{@code gdbAutoReadWriteSplitting} decides where a statement belongs by looking at it, so it needs
 * {@code sqlParser} ahead of it in the chain to have parse results to look at. The region logic it wraps is
 * unchanged, which is what the inherited assertions check: the plugin that picks the destination differently must
 * still apply the same rules once it has picked.
 *
 * <p>{@code setReadOnly} still works here - the auto variant treats it as one signal among two - so the inherited
 * tests remain meaningful without rewriting them around SQL shape.
 */
@ExtendWith(TestDriverProvider.class)
@Order(41)
public class GdbAutoReadWriteSplittingTests extends GdbReadWriteSplittingTests {

  @Override
  protected Properties getProps() {
    final Properties props = getDefaultProps();
    // sqlParser must precede the splitting plugin. Plugin weights order it that way automatically, but naming it
    // is what makes it present at all: without parse results the auto variant routes everything to the writer.
    PropertyDefinition.PLUGINS.set(props, "sqlParser,gdbAutoReadWriteSplitting");
    GlobalAuroraHostListProvider.GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.set(
        props, global().getInstanceHostPatterns());
    return props;
  }
}
