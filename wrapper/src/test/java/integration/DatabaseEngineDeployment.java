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

package integration;

public enum DatabaseEngineDeployment {
  DOCKER,
  RDS,
  RDS_MULTI_AZ_CLUSTER,
  RDS_MULTI_AZ_INSTANCE,
  AURORA,

  /**
   * An Aurora global database: regional clusters in two or more regions under one global cluster.
   *
   * <p>Its own deployment rather than a feature of {@link #AURORA}, because what a test may assume differs.
   * A secondary region holds no writer, so "the writer" is not local; a cluster endpoint exists per region and
   * only one of them is writable; and the topology a driver sees spans regions, which is what the GDB plugins
   * exist to handle. A test written for {@link #AURORA} would quietly assume all three.
   */
  AURORA_GLOBAL
}
