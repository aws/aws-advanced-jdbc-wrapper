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
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.util.Messages;

public class AwsCredentialsManager {
  private static AwsCredentialsProviderHandler handler = null;

  public static synchronized void setCustomHandler(final AwsCredentialsProviderHandler customHandler) {
    handler = customHandler;
  }

  public static synchronized void resetCustomHandler() {
    handler = null;
  }

  public static synchronized AwsCredentialsProvider getProvider(
      final HostSpec hostSpec,
      final Properties props) {
    final AwsCredentialsProvider provider =  handler != null
        ? handler.getAwsCredentialsProvider(hostSpec, props)
        : getDefaultProvider();
    if (provider == null) {
      throw new IllegalArgumentException(Messages.get("AwsCredentialsManager.nullProvider"));
    }
    return provider;
  }

  private static AwsCredentialsProvider getDefaultProvider() {
    return DefaultCredentialsProvider.create();
  }
}
