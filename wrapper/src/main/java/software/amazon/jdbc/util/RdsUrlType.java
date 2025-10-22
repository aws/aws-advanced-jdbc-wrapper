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

public enum RdsUrlType {
  IP_ADDRESS(false, false),
  RDS_WRITER_CLUSTER(true, true),
  RDS_READER_CLUSTER(true, true),
  RDS_CUSTOM_CLUSTER(true, true),
  RDS_PROXY(true, false),
  RDS_INSTANCE(true, false),
  RDS_AURORA_LIMITLESS_DB_SHARD_GROUP(true, false),
  RDS_GLOBAL_WRITER_CLUSTER(true, true),
  OTHER(false, false);

  private final boolean isRds;
  private final boolean isRdsCluster;

  RdsUrlType(final boolean isRds, final boolean isRdsCluster) {
    this.isRds = isRds;
    this.isRdsCluster = isRdsCluster;
  }

  public boolean isRds() {
    return this.isRds;
  }

  public boolean isRdsCluster() {
    return this.isRdsCluster;
  }
}
