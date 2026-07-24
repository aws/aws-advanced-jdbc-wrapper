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

package software.amazon.jdbc.authentication;

import java.util.Properties;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.ResourceLock;
import software.amazon.jdbc.util.StringUtils;

public class AwsCredentialsManager {
  private static @Nullable AwsCredentialsProviderHandler handler = null;

  private static final ResourceLock lock = new ResourceLock();

  public static void setCustomHandler(final @Nullable AwsCredentialsProviderHandler customHandler) {
    try (ResourceLock ignored = lock.obtain()) {
      handler = customHandler;
    }
  }

  public static void resetCustomHandler() {
    try (ResourceLock ignored = lock.obtain()) {
      handler = null;
    }
  }

  public static AwsCredentialsProvider getProvider(final HostSpec hostSpec, final Properties props) {
    return getProvider(hostSpec, props, null);
  }

  /**
   * Resolves an {@link AwsCredentialsProvider} for the given connection, optionally scoping the
   * AWS Sign-In ({@code login_session}) credential flow to the provided region.
   *
   * @param hostSpec the host being connected to.
   * @param props    the connection properties.
   * @param region   the region already resolved for the connection. Used to scope the login/signin
   *                 exchange when {@code allowAwsLoginSession=true} and the configured profile
   *                 authenticates via a {@code login_session} entry. May be null when no region has
   *                 been resolved yet.
   * @return the credentials provider to use for the connection.
   */
  public static AwsCredentialsProvider getProvider(
      final HostSpec hostSpec, final Properties props, final @Nullable Region region) {
    try (ResourceLock ignored = lock.obtain()) {
      AwsCredentialsProvider provider = handler == null
          ? null
          : handler.getAwsCredentialsProvider(hostSpec, props);

      if (provider == null) {
        provider = getDefaultProvider(props, region);
      }

      return provider;
    }
  }

  private static AwsCredentialsProvider getDefaultProvider(
      final Properties props, final @Nullable Region region) {
    final String awsProfileName = PropertyDefinition.AWS_PROFILE.getString(props);

    if (PropertyDefinition.ALLOW_AWS_LOGIN_SESSION.getBoolean(props)) {
      final AwsCredentialsProvider loginProvider =
          LoginCredentialsProviderFactory.create(awsProfileName, region);
      if (loginProvider != null) {
        return loginProvider;
      }
    }

    DefaultCredentialsProvider.Builder builder = DefaultCredentialsProvider.builder();
    if (!StringUtils.isNullOrEmpty(awsProfileName)) {
      builder.profileName(awsProfileName);
    }
    return builder.build();
  }
}
