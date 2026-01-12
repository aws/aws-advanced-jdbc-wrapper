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

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import software.amazon.awssdk.services.rds.model.DBClusterEndpoint;

/**
 * Represents custom endpoint information for a given custom endpoint.
 */
public class CustomEndpointInfo {
  private final String endpointIdentifier; // ID portion of the custom endpoint URL.
  private final String clusterIdentifier; // ID of the cluster that the custom endpoint belongs to.
  private final String url;
  private final CustomEndpointRoleType roleType;

  // A given custom endpoint will either specify a static list or an exclusion list, as indicated by `memberListType`.
  // If the list is a static list, 'members' specifies instances included in the custom endpoint, and new cluster
  // instances will not be automatically added to the custom endpoint. If it is an exclusion list, 'members' specifies
  // instances excluded by the custom endpoint, and new cluster instances will be added to the custom endpoint.
  private final MemberListType memberListType;
  private final Set<String> members;

  /**
   * Constructs a new CustomEndpointInfo instance with the specified details.
   *
   * @param endpointIdentifier The endpoint identifier for the custom endpoint. For example, if the custom endpoint URL
   *                           is "my-custom-endpoint.cluster-custom-XYZ.us-east-1.rds.amazonaws.com", the endpoint
   *                           identifier is "my-custom-endpoint".
   * @param clusterIdentifier  The cluster identifier for the cluster that the custom endpoint belongs to.
   * @param url                The URL for the custom endpoint.
   * @param roleType           The role type of the custom endpoint.
   * @param members            The instance IDs for the hosts in the custom endpoint.
   * @param memberListType     The list type for {@code members}.
   */
  public CustomEndpointInfo(
      String endpointIdentifier,
      String clusterIdentifier,
      String url,
      CustomEndpointRoleType roleType,
      Set<String> members,
      MemberListType memberListType) {
    this.endpointIdentifier = endpointIdentifier;
    this.clusterIdentifier = clusterIdentifier;
    this.url = url;
    this.roleType = roleType;
    this.members = members;
    this.memberListType = memberListType;
  }

  /**
   * Constructs a CustomEndpointInfo object from a DBClusterEndpoint instance as returned by the RDS API.
   *
   * @param responseEndpointInfo The endpoint info returned by the RDS API.
   * @return a CustomEndPointInfo object representing the information in the given DBClusterEndpoint.
   */
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
        CustomEndpointRoleType.valueOf(responseEndpointInfo.customEndpointType()),
        new HashSet<>(members),
        memberListType
    );
  }

  /**
   * Gets the endpoint identifier for the custom endpoint. For example, if the custom endpoint URL is
   * "my-custom-endpoint.cluster-custom-XYZ.us-east-1.rds.amazonaws.com", the endpoint identifier is
   * "my-custom-endpoint".
   *
   * @return the endpoint identifier for the custom endpoint.
   */
  public String getEndpointIdentifier() {
    return endpointIdentifier;
  }

  /**
   * Gets the cluster identifier for the cluster that the custom endpoint belongs to.
   *
   * @return the cluster identifier for the cluster that the custom endpoint belongs to.
   */
  public String getClusterIdentifier() {
    return clusterIdentifier;
  }

  /**
   * Gets the URL for the custom endpoint.
   *
   * @return the URL for the custom endpoint.
   */
  public String getUrl() {
    return url;
  }

  /**
   * Gets the role type of the custom endpoint.
   *
   * @return the role type of the custom endpoint.
   */
  public CustomEndpointRoleType getCustomEndpointType() {
    return roleType;
  }

  /**
   * Gets the member list type of the custom endpoint.
   *
   * @return the member list type of the custom endpoint.
   */
  public MemberListType getMemberListType() {
    return this.memberListType;
  }

  /**
   * Gets the static members of the custom endpoint. If the custom endpoint member list type is an exclusion list,
   * returns null.
   *
   * @return the static members of the custom endpoint, or null if the custom endpoint member list type is an exclusion
   *     list.
   */
  public Set<String> getStaticMembers() {
    return STATIC_LIST.equals(this.memberListType) ? this.members : null;
  }

  /**
   * Gets the excluded members of the custom endpoint. If the custom endpoint member list type is a static list,
   * returns null.
   *
   * @return the excluded members of the custom endpoint, or null if the custom endpoint member list type is a static
   *     list.
   */
  public Set<String> getExcludedMembers() {
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
        && Objects.equals(this.roleType, info.roleType)
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
    result = prime * result + ((this.roleType == null) ? 0 : this.roleType.hashCode());
    result = prime * result + ((this.memberListType == null) ? 0 : this.memberListType.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return String.format(
        "CustomEndpointInfo[url=%s, clusterIdentifier=%s, customEndpointType=%s, memberListType=%s, members=%s]",
        this.url, this.clusterIdentifier, this.roleType, this.memberListType, this.members);
  }
}
