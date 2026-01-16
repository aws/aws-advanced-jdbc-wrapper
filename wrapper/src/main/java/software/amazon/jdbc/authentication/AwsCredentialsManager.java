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
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.ResourceLock;
import software.amazon.jdbc.util.StringUtils;

public class AwsCredentialsManager {
  private static AwsCredentialsProviderHandler handler = null;

  private static final ResourceLock lock = new ResourceLock();

  public static void setCustomHandler(final AwsCredentialsProviderHandler customHandler) {
    try (ResourceLock ignored = lock.obtain()) {
      handler = customHandler;
    }
  }

  public static void resetCustomHandler() {
    try (ResourceLock ignored = lock.obtain()) {
      handler = null;
    }
  }

  public static AwsCredentialsProvider getProvider(
      final HostSpec hostSpec, final Properties props) {
    try (ResourceLock ignored = lock.obtain()) {
      AwsCredentialsProvider provider =
          handler == null ? null : handler.getAwsCredentialsProvider(hostSpec, props);

      if (provider == null) {
        provider = getDefaultProvider(PropertyDefinition.AWS_PROFILE.getString(props));
      }

      return provider;
    }
  }

  private static AwsCredentialsProvider getDefaultProvider(final @Nullable String awsProfileName) {
    DefaultCredentialsProvider.Builder builder = DefaultCredentialsProvider.builder();
    if (!StringUtils.isNullOrEmpty(awsProfileName)) {
      builder.profileName(awsProfileName);
    }
    return builder.build();
  }
}
