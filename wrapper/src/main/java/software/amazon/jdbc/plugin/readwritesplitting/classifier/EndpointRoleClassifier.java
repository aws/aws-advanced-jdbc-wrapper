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

package software.amazon.jdbc.plugin.readwritesplitting.classifier;

import software.amazon.jdbc.HostSpec;

/**
 * {@link RoleClassifier} for endpoint-based (Simple) read/write splitting: a host is the writer or
 * reader if its host (or host:port) matches the configured write/read endpoint string.
 */
public class EndpointRoleClassifier implements RoleClassifier {

  private final String writeEndpoint;
  private final String readEndpoint;

  public EndpointRoleClassifier(final String writeEndpoint, final String readEndpoint) {
    this.writeEndpoint = writeEndpoint;
    this.readEndpoint = readEndpoint;
  }

  @Override
  public boolean isWriter(final HostSpec host) {
    return host != null
        && (this.writeEndpoint.equalsIgnoreCase(host.getHost())
            || this.writeEndpoint.equalsIgnoreCase(host.getHostAndPort()));
  }

  @Override
  public boolean isReader(final HostSpec host) {
    return host != null
        && (this.readEndpoint.equalsIgnoreCase(host.getHost())
            || this.readEndpoint.equalsIgnoreCase(host.getHostAndPort()));
  }
}
