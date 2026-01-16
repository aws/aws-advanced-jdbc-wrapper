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

package software.amazon.jdbc.util;

import java.util.List;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;

public class HostSelectorUtils {
  public static void setHostWeightPairsProperty(
      final @NonNull AwsWrapperProperty property,
      final @NonNull Properties properties,
      final @NonNull List<HostSpec> hosts) {
    final StringBuilder builder = new StringBuilder();
    for (int i = 0; i < hosts.size(); i++) {
      builder.append(hosts.get(i).getHostId()).append(":").append(hosts.get(i).getWeight());
      if (i < hosts.size() - 1) {
        builder.append(",");
      }
    }
    final String hostWeightPairsString = builder.toString();
    properties.setProperty(property.name, hostWeightPairsString);
  }
}
