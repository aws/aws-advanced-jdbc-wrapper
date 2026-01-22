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

import java.util.Properties;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.regions.Region;

public class RegionUtils {
  protected static final RdsUtils rdsUtils = new RdsUtils();

  /**
   * Determines the AWS region from the given parameters. If the region is defined in the properties, that region will
   * be used. Otherwise, attempts to determine the region from the passed in host.
   *
   * @param host    The host from which to extract the region if it is not defined in the properties.
   * @param props   The connection properties for the connection being established.
   * @param propKey The key name of the region property.
   * @return The AWS region defined by the properties or extracted from the host, or null if the region was not
   *     defined in the properties and could not be determined from the {@code host}.
   */
  @Nullable
  public Region getRegion(String host, Properties props, String propKey) {
    Region region = getRegion(props, propKey);
    return region != null ? region : getRegionFromHost(host);
  }

  /**
   * Determines the AWS region from the given properties.
   *
   * @param props   The connection properties for the connection being established.
   * @param propKey The key name of the region property.
   * @return The AWS region defined by the properties, or null if the region was not defined in the properties.
   */
  @Nullable
  public Region getRegion(Properties props, String propKey) {
    String regionString = props.getProperty(propKey);
    if (StringUtils.isNullOrEmpty(regionString)) {
      return null;
    }

    return getRegionFromRegionString(regionString);
  }

  /**
   * Determines the AWS region from the given region string.
   *
   * @param regionString The connection properties for the connection being established.
   * @return The AWS region of the given region string, or null if the given string was null or empty.
   */
  public Region getRegionFromRegionString(String regionString) {
    if (StringUtils.isNullOrEmpty(regionString)) {
      return null;
    }

    final Region region = Region.of(regionString);
    if (!Region.regions().contains(region)) {
      throw new RuntimeException(
          Messages.get(
              "AwsSdk.unsupportedRegion",
              new Object[] {regionString}));
    }

    return region;
  }

  /**
   * Determines the AWS region from the given host string.
   *
   * @param host The host from which to extract the region.
   * @return The AWS region used in the host string.
   */
  public Region getRegionFromHost(String host) {
    String regionString = rdsUtils.getRdsRegion(host);
    if (StringUtils.isNullOrEmpty(regionString)) {
      return null;
    }

    return getRegionFromRegionString(regionString);
  }
}
