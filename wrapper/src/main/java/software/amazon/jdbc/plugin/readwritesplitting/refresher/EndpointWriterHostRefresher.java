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

import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.plugin.readwritesplitting.EndpointHostSpecs;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;

/**
 * {@link TopologyRefresher} for endpoint-based Global Database read/write splitting. There is no
 * cluster topology to refresh, but the writer host spec must be seeded from the configured write
 * endpoint so the Global Database writer resolver can evaluate its region (accessible-region /
 * home-region rules and Global Write Forwarding) before a writer connection is opened.
 */
public class EndpointWriterHostRefresher implements TopologyRefresher {

  private final String writeEndpoint;

  public EndpointWriterHostRefresher(final String writeEndpoint) {
    this.writeEndpoint = writeEndpoint;
  }

  @Override
  public void refresh(final RwSplitContext ctx) {
    if (ctx.writerHostSpec() == null && ctx.hostListProviderService() != null) {
      ctx.setWriterHostSpec(
          EndpointHostSpecs.create(ctx.hostListProviderService(), this.writeEndpoint, HostRole.WRITER));
    }
  }
}
