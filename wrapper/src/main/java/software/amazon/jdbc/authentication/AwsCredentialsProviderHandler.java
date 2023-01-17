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
import software.amazon.jdbc.HostSpec;

/**
 * Interface for selecting an {@link AwsCredentialsProvider} based on the passed in parameters.
 * The AwsCredentialsProviderHandler should be passed to the {@link AwsCredentialsManager} via
 * {@link AwsCredentialsManager#setCustomHandler(AwsCredentialsProviderHandler)
 * AwsCredentialsManager.setCustomHandler}.
 */
@FunctionalInterface
public interface AwsCredentialsProviderHandler {
  AwsCredentialsProvider getAwsCredentialsProvider(HostSpec hostSpec, Properties props);
}
