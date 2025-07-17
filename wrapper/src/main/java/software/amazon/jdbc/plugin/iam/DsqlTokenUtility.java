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
import software.amazon.awssdk.services.dsql.DsqlUtilities;

/**
 * Represents an {@link IamTokenUtility} which provides auth tokens for connecting to Amazon Aurora DSQL.
 */
public class DsqlTokenUtility implements IamTokenUtility {

  private DsqlUtilities utilities = null;

  public DsqlTokenUtility() { }

  // For testing only
  DsqlTokenUtility(@NonNull final DsqlUtilities utilities) {
    this.utilities = utilities;
  }

  @Override
  public String generateAuthenticationToken(
      @NonNull final AwsCredentialsProvider credentialsProvider,
      @NonNull final Region region,
      @NonNull final String hostname,
      final int port,
      @NonNull final String username) {
    if (this.utilities == null) {
      this.utilities = DsqlUtilities.builder()
          .credentialsProvider(credentialsProvider)
          .region(region)
          .build();
    }
    if (username.equals("admin")) {
      return this.utilities.generateDbConnectAdminAuthToken((builder) ->
          builder.hostname(hostname).region(region)
      );
    } else {
      return this.utilities.generateDbConnectAuthToken((builder) ->
          builder.hostname(hostname).region(region)
      );
    }
  }
}
