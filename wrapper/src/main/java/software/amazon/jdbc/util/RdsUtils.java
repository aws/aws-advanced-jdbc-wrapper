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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.checkerframework.checker.nullness.qual.Nullable;

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
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>[a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com(\\.cn)?)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_CLUSTER_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>cluster-|cluster-ro-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>[a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com(\\.cn)?)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_CHINA_DNS_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>proxy-|cluster-|cluster-ro-|cluster-custom-)?"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>rds\\.[a-zA-Z0-9\\-]+|"
              + "[a-zA-Z0-9\\-]+\\.rds)\\.amazonaws\\.com\\.cn)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_CHINA_CLUSTER_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>cluster-|cluster-ro-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>rds\\.[a-zA-Z0-9\\-]+|"
              + "[a-zA-Z0-9\\-]+\\.rds)\\.amazonaws\\.com\\.cn)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern ELB_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\.elb\\."
              + "((?<region>[a-zA-Z0-9\\-]+)\\.amazonaws\\.com)",
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

  private static final Map<String, Matcher> cachedPatterns = new ConcurrentHashMap<>();
  private static final Map<String, String> cachedDnsPatterns = new ConcurrentHashMap<>();

  private static final String INSTANCE_GROUP = "instance";
  private static final String DNS_GROUP = "dns";
  private static final String DOMAIN_GROUP = "domain";
  private static final String REGION_GROUP = "region";

  public boolean isRdsClusterDns(final String host) {
    final String dnsGroup = getDnsGroup(host);
    return dnsGroup != null && (dnsGroup.equalsIgnoreCase("cluster-") || dnsGroup.equalsIgnoreCase("cluster-ro-"));
  }

  public boolean isRdsCustomClusterDns(final String host) {
    final String dnsGroup = getDnsGroup(host);
    return dnsGroup != null && dnsGroup.startsWith("cluster-custom-");
  }

  public boolean isRdsDns(final String host) {
    final Matcher matcher = cacheMatcher(host, AURORA_DNS_PATTERN, AURORA_CHINA_DNS_PATTERN);
    final String group = getRegexGroup(matcher, DNS_GROUP);
    if (group != null) {
      cachedDnsPatterns.put(host, group);
    }

    return matcher != null;
  }

  public boolean isRdsInstance(final String host) {
    return getDnsGroup(host) == null && isRdsDns(host);
  }

  public boolean isRdsProxyDns(final String host) {
    final String dnsGroup = getDnsGroup(host);
    return dnsGroup != null && dnsGroup.startsWith("proxy-");
  }

  public @Nullable String getRdsInstanceId(final String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return null;
    }

    final Matcher matcher = cacheMatcher(host, AURORA_DNS_PATTERN, AURORA_CHINA_DNS_PATTERN);
    if (getRegexGroup(matcher, DNS_GROUP) == null) {
      return getRegexGroup(matcher, INSTANCE_GROUP);
    }

    return null;
  }

  public String getRdsInstanceHostPattern(final String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return "?";
    }

    final Matcher matcher = cacheMatcher(host, AURORA_DNS_PATTERN, AURORA_CHINA_DNS_PATTERN);
    final String group = getRegexGroup(matcher, DOMAIN_GROUP);
    return group == null ? "?" : "?." + group;
  }

  public String getRdsRegion(final String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return null;
    }

    final Matcher matcher = cacheMatcher(host, AURORA_DNS_PATTERN, AURORA_CHINA_DNS_PATTERN);
    final String group = getRegexGroup(matcher, REGION_GROUP);
    if (group != null) {
      // Using replaceAll to strip 'rds' in case it is a China endpoint.
      return group.replaceAll("rds", "").replaceAll("\\.", "");
    }

    final Matcher elbMatcher = ELB_PATTERN.matcher(host);
    if (elbMatcher.find()) {
      return getRegexGroup(elbMatcher, REGION_GROUP);
    }
    return null;
  }

  public boolean isWriterClusterDns(final String host) {
    final String dnsGroup = getDnsGroup(host);
    return dnsGroup != null && dnsGroup.equalsIgnoreCase("cluster-");
  }

  public boolean isReaderClusterDns(final String host) {
    final String dnsGroup = getDnsGroup(host);
    return dnsGroup != null && dnsGroup.equalsIgnoreCase("cluster-ro-");
  }

  public String getRdsClusterHostUrl(final String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return null;
    }

    final Matcher matcher = AURORA_CLUSTER_PATTERN.matcher(host);
    if (matcher.find()) {
      return host.replaceAll(AURORA_CLUSTER_PATTERN.pattern(), "${instance}.cluster-${domain}");
    }
    final Matcher chinaMatcher = AURORA_CHINA_CLUSTER_PATTERN.matcher(host);
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

  public boolean isDnsPatternValid(final String pattern) {
    return pattern.contains("?");
  }

  public RdsUrlType identifyRdsType(final String host) {
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
      // ELB URLs will also be classified as other
      return RdsUrlType.OTHER;
    }
  }

  private Matcher cacheMatcher(final String host, Pattern... patterns) {
    Matcher matcher = null;
    for (Pattern pattern : patterns) {
      matcher = cachedPatterns.get(host);
      if (matcher != null) {
        return matcher;
      }
      matcher = pattern.matcher(host);
      if (matcher.find()) {
        cachedPatterns.put(host, matcher);
        return matcher;
      }
    }
    return null;
  }

  private String getRegexGroup(Matcher matcher, String groupName) {
    if (matcher == null) {
      return null;
    }

    try {
      return matcher.group(groupName);
    } catch (IllegalStateException e) {
      return null;
    }
  }

  private String getDnsGroup(final String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return null;
    }
    return cachedDnsPatterns.computeIfAbsent(host, (k) -> {
      final Matcher matcher = cacheMatcher(k, AURORA_DNS_PATTERN, AURORA_CHINA_DNS_PATTERN);
      return getRegexGroup(matcher, DNS_GROUP);
    });
  }

  public static void clearCache() {
    cachedPatterns.clear();
  }
}
