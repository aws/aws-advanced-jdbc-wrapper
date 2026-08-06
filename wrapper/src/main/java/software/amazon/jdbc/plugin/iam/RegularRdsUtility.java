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

package software.amazon.jdbc.plugin.iam;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;

public class RegularRdsUtility implements IamTokenUtility {

  // Cache the RdsUtilities per region. RdsUtilities bakes in the region (and credentials provider)
  // at build time, so a single cached instance would produce tokens signed for the region seen on
  // the first call even after the region changes (e.g. cross-region failover). Keying by region
  // ensures each region uses an RdsUtilities built for that region. The map is naturally bounded by
  // the small number of regions a single instance can encounter.
  private final Map<Region, RdsUtilities> utilitiesByRegion = new ConcurrentHashMap<>();

  // For testing only: when set, this instance is always used regardless of region so tests can
  // inject an RdsUtilities backed by a fixed clock for deterministic tokens.
  private final @Nullable RdsUtilities injectedUtilities;

  public RegularRdsUtility() {
    this.injectedUtilities = null;
  }

  // For testing only
  public RegularRdsUtility(final RdsUtilities utilities) {
    this.injectedUtilities = utilities;
  }

  @Override
  public String generateAuthenticationToken(
      final @NonNull AwsCredentialsProvider credentialsProvider,
      final @NonNull Region region,
      final @NonNull String hostname,
      final int port,
      final @NonNull String username) {

    final RdsUtilities localUtilities = this.injectedUtilities != null
        ? this.injectedUtilities
        : this.utilitiesByRegion.computeIfAbsent(
            region,
            r -> RdsUtilities.builder()
                .credentialsProvider(credentialsProvider)
                .region(r)
                .build());

    return localUtilities.generateAuthenticationToken((builder) ->
        builder
            .hostname(hostname)
            .port(port)
            .username(username)
    );
  }
}
