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

package software.amazon.jdbc.plugin.federatedauth;

import static software.amazon.jdbc.plugin.federatedauth.FederatedAuthPlugin.IAM_IDP_ARN;
import static software.amazon.jdbc.plugin.federatedauth.FederatedAuthPlugin.IAM_ROLE_ARN;

import java.sql.SQLException;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleWithSamlCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithSamlRequest;

public abstract class SamlCredentialsProviderFactory implements CredentialsProviderFactory {

  @Override
  public AwsCredentialsProvider getAwsCredentialsProvider(final String host, final Region region,
      final @NonNull Properties props)
      throws SQLException {

    final String samlAssertion = getSamlAssertion(props);

    final AssumeRoleWithSamlRequest assumeRoleWithSamlRequest =  AssumeRoleWithSamlRequest.builder()
        .samlAssertion(samlAssertion)
        .roleArn(IAM_ROLE_ARN.getString(props))
        .principalArn(IAM_IDP_ARN.getString(props))
        .build();

    final StsClient stsClient = StsClient.builder()
        .credentialsProvider(AnonymousCredentialsProvider.create())
        .region(region)
        .build();

    return StsAssumeRoleWithSamlCredentialsProvider.builder()
        .refreshRequest(assumeRoleWithSamlRequest)
        .asyncCredentialUpdateEnabled(true)
        .stsClient(stsClient)
        .build();
  }

  abstract String getSamlAssertion(final @NonNull Properties props) throws SQLException;
}
