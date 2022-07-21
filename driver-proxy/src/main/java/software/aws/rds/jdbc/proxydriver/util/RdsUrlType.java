/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.util;

public enum RdsUrlType {
  IP_ADDRESS(false, false),
  RDS_WRITER_CLUSTER(true, true),
  RDS_READER_CLUSTER(true, true),
  RDS_CUSTOM_CLUSTER(true, true),
  RDS_PROXY(true, false),
  RDS_INSTANCE(true, false),
  OTHER(false, false);

  private final boolean isRds;
  private final boolean isRdsCluster;

  RdsUrlType(boolean isRds, boolean isRdsCluster) {
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
