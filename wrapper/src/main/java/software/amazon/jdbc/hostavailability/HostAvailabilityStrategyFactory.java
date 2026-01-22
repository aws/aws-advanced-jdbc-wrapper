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

package software.amazon.jdbc.hostavailability;

import java.util.Properties;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.StringUtils;

public class HostAvailabilityStrategyFactory {

  public static final AwsWrapperProperty DEFAULT_HOST_AVAILABILITY_STRATEGY = new AwsWrapperProperty(
      "defaultHostAvailabilityStrategy", "",
      "An override for specifying the default host availability change strategy.");

  public static final AwsWrapperProperty HOST_AVAILABILITY_STRATEGY_MAX_RETRIES = new AwsWrapperProperty(
      "hostAvailabilityStrategyMaxRetries", "5",
      "Max number of retries for checking a host's availability.");

  public static final AwsWrapperProperty HOST_AVAILABILITY_STRATEGY_INITIAL_BACKOFF_TIME = new AwsWrapperProperty(
      "hostAvailabilityStrategyInitialBackoffTime", "30",
      "The initial backoff time in seconds.");

  static {
    PropertyDefinition.registerPluginProperties(HostAvailabilityStrategyFactory.class);
  }

  public HostAvailabilityStrategy create(Properties props) {
    if (props == null || StringUtils.isNullOrEmpty(DEFAULT_HOST_AVAILABILITY_STRATEGY.getString(props))) {
      return new SimpleHostAvailabilityStrategy();
    } else if (ExponentialBackoffHostAvailabilityStrategy.NAME
        .equalsIgnoreCase(DEFAULT_HOST_AVAILABILITY_STRATEGY.getString(props))) {
      return new ExponentialBackoffHostAvailabilityStrategy(props);
    }
    return new SimpleHostAvailabilityStrategy();
  }
}
