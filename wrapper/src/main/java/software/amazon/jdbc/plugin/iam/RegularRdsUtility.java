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

import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;

public class RegularRdsUtility implements IamTokenUtility {

  @Override
  public String generateAuthenticationToken(
      final @NonNull AwsCredentialsProvider credentialsProvider,
      final @NonNull Region region,
      final @NonNull String hostname,
      final int port,
      final @NonNull String username) {

    final RdsUtilities utilities = RdsUtilities.builder()
        .credentialsProvider(credentialsProvider)
        .region(region)
        .build();
    return utilities.generateAuthenticationToken((builder) ->
        builder
            .hostname(hostname)
            .port(port)
            .username(username)
    );
  }
}
