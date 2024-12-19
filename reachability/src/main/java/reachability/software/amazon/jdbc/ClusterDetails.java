package reachability.software.amazon.jdbc;

import software.amazon.jdbc.util.StringUtils;

public class ClusterDetails {
  public String endpoint = System.getenv("CLUSTER_ENDPOINT");
  public String region = System.getenv("CLUSTER_REGION");
  public String iamUser = System.getenv("IAM_USER");
  public String username = System.getenv("CLUSTER_USERNAME");
  public String password = System.getenv("CLUSTER_PASSWORD");
  public String databaseName = System.getenv("CLUSTER_DATABASE_NAME");

  public ClusterDetails() {}

  @Override
  public String toString() {
    return String.format("%s: endpoint=%s, region=%s, iamUser=%s, username=%s, password=%s, databaseName=%s",
        super.toString(), this.endpoint, this.region, this.iamUser, this.username,
        StringUtils.isNullOrEmpty(this.password) ? "<empty>" : "<masked>", this.databaseName);
  }
}
