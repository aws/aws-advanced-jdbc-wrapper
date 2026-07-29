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

import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.util.Messages;

/**
 * Builds a {@link HostSpec} from a configured endpoint string (parsing an optional {@code :port}),
 * reproducing the legacy {@code SimpleReadWriteSplittingPlugin.createHostSpec}.
 */
public final class EndpointHostSpecs {

  private EndpointHostSpecs() {
  }

  /**
   * Builds a {@link HostSpec} for the given endpoint.
   *
   * @param hostListProviderService the host list provider service, established by
   *                                {@code initHostProvider}; must not be {@code null}
   * @param endpoint                the configured endpoint, optionally including {@code :port}
   * @param role                    the role to assign to the resulting host
   * @return the host specification for the endpoint
   * @throws IllegalStateException if the host list provider service has not been established yet
   */
  public static HostSpec create(
      final @Nullable HostListProviderService hostListProviderService,
      final String endpoint,
      final HostRole role) {

    if (hostListProviderService == null) {
      // Previously this dereferenced null; fail with an explicit message instead. The service is
      // established by initHostProvider before any read/write splitting routing happens.
      throw new IllegalStateException(Messages.get(
          "EndpointHostSpecs.missingHostListProviderService", new Object[] {endpoint}));
    }

    final String trimmed = endpoint.trim();

    String host = trimmed;
    int port = hostListProviderService.getCurrentHostSpec().getPort();
    final int colonIndex = trimmed.lastIndexOf(":");
    if (colonIndex != -1 && trimmed.substring(colonIndex + 1).matches("\\d+")) {
      host = trimmed.substring(0, colonIndex);
      port = Integer.parseInt(trimmed.substring(colonIndex + 1));
    }

    return new HostSpecBuilder(hostListProviderService.getHostSpecBuilder())
        .host(host)
        .port(port)
        .role(role)
        .availability(HostAvailability.AVAILABLE)
        .build();
  }
}
