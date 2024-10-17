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

/**
 * Enum representing the member list type of a custom endpoint. This information can be used together with a member list
 * to determine which instances are included or excluded from a custom endpoint.
 */
public enum MemberListType {
  /**
   * The member list for the custom endpoint specifies which instances are included in the custom endpoint. If new
   * instances are added to the cluster, they will not be automatically added to the custom endpoint.
   */
  STATIC_LIST,
  /**
   * The member list for the custom endpoint specifies which instances are excluded from the custom endpoint. If new
   * instances are added to the cluster, they will be automatically added to the custom endpoint.
   */
  EXCLUSION_LIST
}
