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

package software.amazon.jdbc.plugin.customendpoint;

import static software.amazon.jdbc.plugin.customendpoint.MemberListType.EXCLUSION_LIST;
import static software.amazon.jdbc.plugin.customendpoint.MemberListType.STATIC_LIST;

import java.util.List;
import java.util.Objects;
import software.amazon.awssdk.services.rds.model.DBClusterEndpoint;

public class CustomEndpointInfo {
  private final String endpointIdentifier; // ID portion of the custom cluster endpoint URL.
  private final String clusterIdentifier; // ID of the cluster that the custom cluster endpoint belongs to.
  private final String url;
  private final CustomEndpointType customEndpointType;

  // A given custom endpoint will either specify a static list or an exclusion list, as indicated by `memberListType`.
  // If the list is a static list, new cluster instances will not be added to the custom endpoint. If it is an exclusion
  // list, new cluster instances will be added to the custom endpoint.
  private final List<String> members;
  private final MemberListType memberListType;

  CustomEndpointInfo(
      String endpointIdentifier,
      String clusterIdentifier,
      String url,
      CustomEndpointType customEndpointType,
      List<String> members,
      MemberListType memberListType) {
    this.endpointIdentifier = endpointIdentifier;
    this.clusterIdentifier = clusterIdentifier;
    this.url = url;
    this.customEndpointType = customEndpointType;
    this.members = members;
    this.memberListType = memberListType;
  }

  public static CustomEndpointInfo fromDBClusterEndpoint(DBClusterEndpoint responseEndpointInfo) {
    final List<String> members;
    final MemberListType memberListType;

    if (responseEndpointInfo.hasStaticMembers()) {
      members = responseEndpointInfo.staticMembers();
      memberListType = MemberListType.STATIC_LIST;
    } else {
      members = responseEndpointInfo.excludedMembers();
      memberListType = MemberListType.EXCLUSION_LIST;
    }

    return new CustomEndpointInfo(
        responseEndpointInfo.dbClusterEndpointIdentifier(),
        responseEndpointInfo.dbClusterIdentifier(),
        responseEndpointInfo.endpoint(),
        CustomEndpointType.valueOf(responseEndpointInfo.customEndpointType()),
        members,
        memberListType
    );
  }

  public String getEndpointIdentifier() {
    return endpointIdentifier;
  }

  public String getClusterIdentifier() {
    return clusterIdentifier;
  }

  public String getUrl() {
    return url;
  }

  public CustomEndpointType getCustomEndpointType() {
    return customEndpointType;
  }

  public MemberListType getMemberListType() {
    return this.memberListType;
  }

  public List<String> getStaticMembers() {
    return STATIC_LIST.equals(this.memberListType) ? this.members : null;
  }

  public List<String> getExcludedMembers() {
    return EXCLUSION_LIST.equals(this.memberListType) ? this.members : null;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (getClass() != obj.getClass()) {
      return false;
    }

    CustomEndpointInfo info = (CustomEndpointInfo) obj;
    return Objects.equals(this.endpointIdentifier, info.endpointIdentifier)
        && Objects.equals(this.clusterIdentifier, info.clusterIdentifier)
        && Objects.equals(this.url, info.url)
        && Objects.equals(this.customEndpointType, info.customEndpointType)
        && Objects.equals(this.members, info.members)
        && Objects.equals(this.memberListType, info.memberListType);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((this.endpointIdentifier == null) ? 0 : this.endpointIdentifier.hashCode());
    result = prime * result + ((this.clusterIdentifier == null) ? 0 : this.clusterIdentifier.hashCode());
    result = prime * result + ((this.url == null) ? 0 : this.url.hashCode());
    result = prime * result + ((this.customEndpointType == null) ? 0 : this.customEndpointType.hashCode());
    result = prime * result + ((this.memberListType == null) ? 0 : this.memberListType.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return String.format(
        "CustomEndpointInfo[url=%s, clusterIdentifier=%s, customEndpointType=%s, memberListType=%s, members=%s]",
        this.url, this.clusterIdentifier, this.customEndpointType, this.memberListType, this.members);
  }
}
