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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RdsUtils {

  // Aurora DB clusters support different endpoints. More details about Aurora RDS endpoints
  // can be found at
  // https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Overview.Endpoints.html
  //
  // Details how to use RDS Proxy endpoints can be found at
  // https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/rds-proxy-endpoints.html
  //
  // Values like "<...>" depend on particular Aurora cluster.
  // For example: "<database-cluster-name>"
  //
  //
  //
  // Cluster (Writer) Endpoint: <database-cluster-name>.cluster-<xyz>.<aws-region>.rds.amazonaws.com
  // Example: test-postgres.cluster-123456789012.us-east-2.rds.amazonaws.com
  //
  // Cluster Reader Endpoint: <database-cluster-name>.cluster-ro-<xyz>.<aws-region>.rds.amazonaws.com
  // Example: test-postgres.cluster-ro-123456789012.us-east-2.rds.amazonaws.com
  //
  // Cluster Custom Endpoint: <cluster-name-alias>.cluster-custom-<xyz>.<aws-region>.rds.amazonaws.com
  // Example: test-postgres-alias.cluster-custom-123456789012.us-east-2.rds.amazonaws.com
  //
  // Instance Endpoint: <instance-name>.<xyz>.<aws-region>.rds.amazonaws.com
  // Example: test-postgres-instance-1.123456789012.us-east-2.rds.amazonaws.com
  //
  //
  //
  // Similar endpoints for China regions have different structure and are presented below.
  //
  // Cluster (Writer) Endpoint: <database-cluster-name>.cluster-<xyz>.rds.<aws-region>.amazonaws.com.cn
  // Example: test-postgres.cluster-123456789012.rds.cn-northwest-1.amazonaws.com.cn
  //
  // Cluster Reader Endpoint: <database-cluster-name>.cluster-ro-<xyz>.rds.<aws-region>.amazonaws.com.cn
  // Example: test-postgres.cluster-ro-123456789012.rds.cn-northwest-1.amazonaws.com.cn
  //
  // Cluster Custom Endpoint: <cluster-name-alias>.cluster-custom-<xyz>.rds.<aws-region>.amazonaws.com.cn
  // Example: test-postgres-alias.cluster-custom-123456789012.rds.cn-northwest-1.amazonaws.com.cn
  //
  // Instance Endpoint: <instance-name>.<xyz>.rds.<aws-region>.amazonaws.com.cn
  // Example: test-postgres-instance-1.123456789012.rds.cn-northwest-1.amazonaws.com.cn

  private static final Pattern AURORA_DNS_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>proxy-|cluster-|cluster-ro-|cluster-custom-)?"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>[a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_INSTANCE_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\.(?<domain>[a-zA-Z0-9]+\\.(?<region>[a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_CLUSTER_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>cluster-|cluster-ro-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>[a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern AURORA_CUSTOM_CLUSTER_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>cluster-custom-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>[a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_PROXY_DNS_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>proxy-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>[a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_CHINA_DNS_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>proxy-|cluster-|cluster-ro-|cluster-custom-)?"
              + "(?<domain>[a-zA-Z0-9]+\\.rds\\.(?<region>[a-zA-Z0-9\\-]+)\\.amazonaws\\.com\\.cn)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_CHINA_INSTANCE_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>[a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com\\.cn)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_CHINA_CLUSTER_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>cluster-|cluster-ro-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.rds\\.(?<region>[a-zA-Z0-9\\-]+)\\.amazonaws\\.com\\.cn)",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern AURORA_CHINA_CUSTOM_CLUSTER_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>cluster-custom-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.rds\\.(?<region>[a-zA-Z0-9\\-]+)\\.amazonaws\\.com\\.cn)",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern AURORA_CHINA_PROXY_DNS_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>proxy-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.rds\\.(?<region>[a-zA-Z0-9\\-])+\\.amazonaws\\.com\\.cn)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern IP_V4 =
      Pattern.compile(
          "^(([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){1}"
              + "(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){2}"
              + "([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$");
  private static final Pattern IP_V6 = Pattern.compile("^[0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){7}$");
  private static final Pattern IP_V6_COMPRESSED =
      Pattern.compile(
          "^(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)"
              + "::(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)$");
  private static final String DNS_GROUP = "dns";
  private static final String DOMAIN_GROUP = "domain";
  private static final String REGION_GROUP = "region";

  public boolean isRdsClusterDns(String host) {
    return !StringUtils.isNullOrEmpty(host)
        && (AURORA_CLUSTER_PATTERN.matcher(host).find() || AURORA_CHINA_CLUSTER_PATTERN.matcher(host).find());
  }

  public boolean isRdsCustomClusterDns(String host) {
    return !StringUtils.isNullOrEmpty(host)
        && (AURORA_CUSTOM_CLUSTER_PATTERN.matcher(host).find()
        || AURORA_CHINA_CUSTOM_CLUSTER_PATTERN.matcher(host).find());
  }

  public boolean isRdsDns(String host) {
    return !StringUtils.isNullOrEmpty(host)
        && (AURORA_DNS_PATTERN.matcher(host).find() || AURORA_CHINA_DNS_PATTERN.matcher(host).find());
  }

  public boolean isRdsInstance(String host) {
    return !StringUtils.isNullOrEmpty(host)
        && (AURORA_INSTANCE_PATTERN.matcher(host).find() || AURORA_CHINA_INSTANCE_PATTERN.matcher(host).find());
  }

  public boolean isRdsProxyDns(String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return false;
    }
    return AURORA_PROXY_DNS_PATTERN.matcher(host).find() || AURORA_CHINA_PROXY_DNS_PATTERN.matcher(host).find();
  }

  public String getRdsInstanceHostPattern(String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return "?";
    }

    Matcher matcher = AURORA_DNS_PATTERN.matcher(host);
    if (matcher.find()) {
      return "?." + matcher.group(DOMAIN_GROUP);
    }
    Matcher chinaMatcher = AURORA_CHINA_DNS_PATTERN.matcher(host);
    if (chinaMatcher.find()) {
      return "?." + chinaMatcher.group(DOMAIN_GROUP);
    }
    return "?";
  }

  public String getRdsRegion(String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return null;
    }

    Matcher matcher = AURORA_DNS_PATTERN.matcher(host);
    if (matcher.find()) {
      return matcher.group(REGION_GROUP);
    }
    Matcher chinaMatcher = AURORA_CHINA_DNS_PATTERN.matcher(host);
    if (chinaMatcher.find()) {
      return chinaMatcher.group(REGION_GROUP);
    }
    return null;
  }

  public boolean isWriterClusterDns(String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return false;
    }

    final Matcher matcher = AURORA_CLUSTER_PATTERN.matcher(host);
    if (matcher.find()) {
      return "cluster-".equalsIgnoreCase(matcher.group(DNS_GROUP));
    }
    final Matcher chinaMatcher = AURORA_CHINA_CLUSTER_PATTERN.matcher(host);
    if (chinaMatcher.find()) {
      return "cluster-".equalsIgnoreCase(chinaMatcher.group(DNS_GROUP));
    }
    return false;
  }

  public boolean isReaderClusterDns(String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return false;
    }

    final Matcher matcher = AURORA_CLUSTER_PATTERN.matcher(host);
    if (matcher.find()) {
      return "cluster-ro-".equalsIgnoreCase(matcher.group(DNS_GROUP));
    }
    final Matcher chinaMatcher = AURORA_CHINA_CLUSTER_PATTERN.matcher(host);
    if (chinaMatcher.find()) {
      return "cluster-ro-".equalsIgnoreCase(chinaMatcher.group(DNS_GROUP));
    }
    return false;
  }

  public String getRdsClusterHostUrl(String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return null;
    }

    Matcher matcher = AURORA_CLUSTER_PATTERN.matcher(host);
    if (matcher.find()) {
      return host.replaceAll(AURORA_CLUSTER_PATTERN.pattern(), "${instance}.cluster-${domain}");
    }
    Matcher chinaMatcher = AURORA_CHINA_CLUSTER_PATTERN.matcher(host);
    if (chinaMatcher.find()) {
      return host.replaceAll(AURORA_CHINA_CLUSTER_PATTERN.pattern(), "${instance}.cluster-${domain}");
    }
    return null;
  }

  public boolean isIPv4(final String ip) {
    return !StringUtils.isNullOrEmpty(ip) && IP_V4.matcher(ip).matches();
  }

  public boolean isIPv6(final String ip) {
    return !StringUtils.isNullOrEmpty(ip) && IP_V6.matcher(ip).matches() || IP_V6_COMPRESSED.matcher(ip).matches();
  }

  public boolean isDnsPatternValid(String pattern) {
    return pattern.contains("?");
  }

  public RdsUrlType identifyRdsType(String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return RdsUrlType.OTHER;
    }

    if (isIPv4(host) || isIPv6(host)) {
      return RdsUrlType.IP_ADDRESS;
    } else if (isWriterClusterDns(host)) {
      return RdsUrlType.RDS_WRITER_CLUSTER;
    } else if (isReaderClusterDns(host)) {
      return RdsUrlType.RDS_READER_CLUSTER;
    } else if (isRdsCustomClusterDns(host)) {
      return RdsUrlType.RDS_CUSTOM_CLUSTER;
    } else if (isRdsProxyDns(host)) {
      return RdsUrlType.RDS_PROXY;
    } else if (isRdsDns(host)) {
      return RdsUrlType.RDS_INSTANCE;
    } else {
      return RdsUrlType.OTHER;
    }
  }
}
