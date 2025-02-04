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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
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
  // https://docs.amazonaws.cn/en_us/aws/latest/userguide/endpoints-Ningxia.html
  // https://docs.amazonaws.cn/en_us/aws/latest/userguide/endpoints-Beijing.html
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
  //
  //
  //
  // Governmental endpoints
  // https://aws.amazon.com/compliance/fips/#FIPS_Endpoints_by_Service
  // https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/Region.html

  private static final Pattern AURORA_DNS_PATTERN =
      Pattern.compile(
          "^(?<instance>.+)\\."
              + "(?<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-)?"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>[a-zA-Z0-9\\-]+)"
              + "\\.rds\\.amazonaws\\.com)$",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_CLUSTER_PATTERN =
      Pattern.compile(
          "^(?<instance>.+)\\."
              + "(?<dns>cluster-|cluster-ro-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>[a-zA-Z0-9\\-]+)"
              + "\\.rds\\.amazonaws\\.com)$",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern AURORA_LIMITLESS_CLUSTER_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>shardgrp-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>[a-zA-Z0-9\\-]+)"
              + "\\.rds\\.(amazonaws\\.com(\\.cn)?|sc2s\\.sgov\\.gov|c2s\\.ic\\.gov))$",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern AURORA_CHINA_DNS_PATTERN =
      Pattern.compile(
          "^(?<instance>.+)\\."
              + "(?<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-)?"
              + "(?<domain>[a-zA-Z0-9]+\\.rds\\.(?<region>[a-zA-Z0-9\\-]+)"
              + "\\.amazonaws\\.com\\.cn)$",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_CHINA_CLUSTER_PATTERN =
      Pattern.compile(
          "^(?<instance>.+)\\."
              + "(?<dns>cluster-|cluster-ro-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.rds\\.(?<region>[a-zA-Z0-9\\-]+)"
              + "\\.amazonaws\\.com\\.cn)$",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_OLD_CHINA_DNS_PATTERN =
      Pattern.compile(
          "^(?<instance>.+)\\."
              + "(?<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-)?"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>[a-zA-Z0-9\\-]+)"
              + "\\.rds\\.amazonaws\\.com\\.cn)$",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_OLD_CHINA_CLUSTER_PATTERN =
      Pattern.compile(
          "^(?<instance>.+)\\."
              + "(?<dns>cluster-|cluster-ro-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>[a-zA-Z0-9\\-]+)"
              + "\\.rds\\.amazonaws\\.com\\.cn)$",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_GOV_DNS_PATTERN =
      Pattern.compile(
          "^(?<instance>.+)\\."
              + "(?<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-)?"
              + "(?<domain>[a-zA-Z0-9]+\\.rds\\.(?<region>[a-zA-Z0-9\\-]+)"
              + "\\.(amazonaws\\.com|c2s\\.ic\\.gov|sc2s\\.sgov\\.gov))$",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_GOV_CLUSTER_PATTERN =
      Pattern.compile(
          "^(?<instance>.+)\\."
              + "(?<dns>cluster-|cluster-ro-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.rds\\.(?<region>[a-zA-Z0-9\\-]+)"
              + "\\.(amazonaws\\.com|c2s\\.ic\\.gov|sc2s\\.sgov\\.gov))$",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern ELB_PATTERN =
      Pattern.compile(
          "^(?<instance>.+)\\.elb\\."
              + "((?<region>[a-zA-Z0-9\\-]+)\\.amazonaws\\.com)$",
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

  private static final Pattern BG_GREEN_HOST_PATTERN =
      Pattern.compile(
          ".*(?<prefix>-green-[0-9a-z]{6})\\..*",
          Pattern.CASE_INSENSITIVE);

  // TODO: check GDB writer endpoint for China regions
  private static final Pattern AURORA_GLOBAL_WRITER_DNS_PATTERN =
      Pattern.compile(
          "^(?<instance>.+)\\."
              + "(?<dns>global-)?"
              + "(?<domain>[a-zA-Z0-9]+\\.global\\.rds\\.amazonaws\\.com)$",
          Pattern.CASE_INSENSITIVE);

  private static final Map<String, Matcher> cachedPatterns = new ConcurrentHashMap<>();
  private static final Map<String, String> cachedDnsPatterns = new ConcurrentHashMap<>();

  private static final String INSTANCE_GROUP = "instance";
  private static final String DNS_GROUP = "dns";
  private static final String DOMAIN_GROUP = "domain";
  private static final String REGION_GROUP = "region";
  private static final AtomicReference<Function<String, String>> prepareHostFunc = new AtomicReference<>(null);

  public boolean isRdsClusterDns(final String host) {
    final String dnsGroup = getDnsGroup(getPreparedHost(host));
    return dnsGroup != null && (dnsGroup.equalsIgnoreCase("cluster-") || dnsGroup.equalsIgnoreCase("cluster-ro-"));
  }

  public boolean isRdsCustomClusterDns(final String host) {
    final String dnsGroup = getDnsGroup(getPreparedHost(host));
    return dnsGroup != null && dnsGroup.startsWith("cluster-custom-");
  }

  public boolean isRdsDns(final String host) {
    final String preparedHost = getPreparedHost(host);
    final Matcher matcher = cacheMatcher(preparedHost,
        AURORA_DNS_PATTERN, AURORA_CHINA_DNS_PATTERN, AURORA_OLD_CHINA_DNS_PATTERN, AURORA_GOV_DNS_PATTERN);
    final String group = getRegexGroup(matcher, DNS_GROUP);
    if (group != null) {
      cachedDnsPatterns.put(preparedHost, group);
    }

    return matcher != null;
  }

  public boolean isRdsInstance(final String host) {
    final String preparedHost = getPreparedHost(host);
    return getDnsGroup(preparedHost) == null && isRdsDns(preparedHost);
  }

  public boolean isRdsProxyDns(final String host) {
    final String dnsGroup = getDnsGroup(getPreparedHost(host));
    return dnsGroup != null && dnsGroup.startsWith("proxy-");
  }

  public @Nullable String getRdsClusterId(final String host) {
    final String preparedHost = getPreparedHost(host);
    if (StringUtils.isNullOrEmpty(preparedHost)) {
      return null;
    }

    final Matcher matcher = cacheMatcher(preparedHost,
        AURORA_DNS_PATTERN, AURORA_CHINA_DNS_PATTERN, AURORA_OLD_CHINA_DNS_PATTERN, AURORA_GOV_DNS_PATTERN);
    if (getRegexGroup(matcher, DNS_GROUP) != null) {
      return getRegexGroup(matcher, INSTANCE_GROUP);
    }

    return null;
  }

  public @Nullable String getRdsInstanceId(final String host) {
    final String preparedHost = getPreparedHost(host);
    if (StringUtils.isNullOrEmpty(preparedHost)) {
      return null;
    }

    final Matcher matcher = cacheMatcher(preparedHost,
        AURORA_DNS_PATTERN, AURORA_CHINA_DNS_PATTERN, AURORA_OLD_CHINA_DNS_PATTERN, AURORA_GOV_DNS_PATTERN);
    if (getRegexGroup(matcher, DNS_GROUP) == null) {
      return getRegexGroup(matcher, INSTANCE_GROUP);
    }

    return null;
  }

  public String getRdsInstanceHostPattern(final String host) {
    final String preparedHost = getPreparedHost(host);
    if (StringUtils.isNullOrEmpty(preparedHost)) {
      return "?";
    }

    final Matcher matcher = cacheMatcher(preparedHost,
        AURORA_DNS_PATTERN, AURORA_CHINA_DNS_PATTERN, AURORA_OLD_CHINA_DNS_PATTERN, AURORA_GOV_DNS_PATTERN);
    final String group = getRegexGroup(matcher, DOMAIN_GROUP);
    return group == null ? "?" : "?." + group;
  }

  public String getRdsRegion(final String host) {
    final String preparedHost = getPreparedHost(host);
    if (StringUtils.isNullOrEmpty(preparedHost)) {
      return null;
    }

    final Matcher matcher = cacheMatcher(preparedHost,
        AURORA_DNS_PATTERN, AURORA_CHINA_DNS_PATTERN, AURORA_OLD_CHINA_DNS_PATTERN, AURORA_GOV_DNS_PATTERN);
    final String group = getRegexGroup(matcher, REGION_GROUP);
    if (group != null) {
      return group;
    }

    final Matcher elbMatcher = ELB_PATTERN.matcher(preparedHost);
    if (elbMatcher.find()) {
      return getRegexGroup(elbMatcher, REGION_GROUP);
    }
    return null;
  }

  public boolean isWriterClusterDns(final String host) {
    final String dnsGroup = getDnsGroup(getPreparedHost(host));
    return dnsGroup != null && dnsGroup.equalsIgnoreCase("cluster-");
  }

  public boolean isReaderClusterDns(final String host) {
    final String dnsGroup = getDnsGroup(getPreparedHost(host));
    return dnsGroup != null && dnsGroup.equalsIgnoreCase("cluster-ro-");
  }

  public boolean isLimitlessDbShardGroupDns(final String host) {
    final String dnsGroup = getDnsGroup(getPreparedHost(host));
    return dnsGroup != null && dnsGroup.equalsIgnoreCase("shardgrp-");
  }

  public String getRdsClusterHostUrl(final String host) {
    final String preparedHost = getPreparedHost(host);
    if (StringUtils.isNullOrEmpty(preparedHost)) {
      return null;
    }

    final Matcher matcher = AURORA_CLUSTER_PATTERN.matcher(preparedHost);
    if (matcher.find()) {
      return preparedHost.replaceAll(AURORA_CLUSTER_PATTERN.pattern(), "${instance}.cluster-${domain}");
    }
    final Matcher chinaMatcher = AURORA_CHINA_CLUSTER_PATTERN.matcher(preparedHost);
    if (chinaMatcher.find()) {
      return preparedHost.replaceAll(AURORA_CHINA_CLUSTER_PATTERN.pattern(), "${instance}.cluster-${domain}");
    }
    final Matcher oldChinaMatcher = AURORA_OLD_CHINA_CLUSTER_PATTERN.matcher(preparedHost);
    if (oldChinaMatcher.find()) {
      return preparedHost.replaceAll(AURORA_OLD_CHINA_CLUSTER_PATTERN.pattern(), "${instance}.cluster-${domain}");
    }
    final Matcher govMatcher = AURORA_GOV_CLUSTER_PATTERN.matcher(preparedHost);
    if (govMatcher.find()) {
      return preparedHost.replaceAll(AURORA_GOV_CLUSTER_PATTERN.pattern(), "${instance}.cluster-${domain}");
    }
    final Matcher limitlessMatcher = AURORA_LIMITLESS_CLUSTER_PATTERN.matcher(preparedHost);
    if (limitlessMatcher.find()) {
      return preparedHost.replaceAll(AURORA_LIMITLESS_CLUSTER_PATTERN.pattern(), "${instance}.cluster-${domain}");
    }
    return null;
  }

  public boolean isGlobalDbWriterClusterDns(final String host) {
    final String dnsGroup = getDnsGroup(getPreparedHost(host));
    return dnsGroup != null && dnsGroup.equalsIgnoreCase("global-");
  }

  public boolean isIPv4(final String ip) {
    return !StringUtils.isNullOrEmpty(ip) && IP_V4.matcher(ip).matches();
  }

  public boolean isIPv6(final String ip) {
    return !StringUtils.isNullOrEmpty(ip)
        && (IP_V6.matcher(ip).matches() || IP_V6_COMPRESSED.matcher(ip).matches());
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
    } else if (isGlobalDbWriterClusterDns(host)) {
      return RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER;
    } else if (isWriterClusterDns(host)) {
      return RdsUrlType.RDS_WRITER_CLUSTER;
    } else if (isReaderClusterDns(host)) {
      return RdsUrlType.RDS_READER_CLUSTER;
    } else if (isRdsCustomClusterDns(host)) {
      return RdsUrlType.RDS_CUSTOM_CLUSTER;
    } else if (isLimitlessDbShardGroupDns(host)) {
      return RdsUrlType.RDS_AURORA_LIMITLESS_DB_SHARD_GROUP;
    } else if (isRdsProxyDns(host)) {
      return RdsUrlType.RDS_PROXY;
    } else if (isRdsDns(host)) {
      return RdsUrlType.RDS_INSTANCE;
    } else {
      // ELB URLs will also be classified as other
      return RdsUrlType.OTHER;
    }
  }

  public String removePort(final String hostAndPort) {
    if (StringUtils.isNullOrEmpty(hostAndPort)) {
      return hostAndPort;
    }
    int index = hostAndPort.indexOf(":");
    if (index == -1) {
      return hostAndPort;
    }
    return hostAndPort.substring(0, index);
  }

  public boolean isGreenInstance(final String host) {
    final String preparedHost = getPreparedHost(host);
    return !StringUtils.isNullOrEmpty(preparedHost) && BG_GREEN_HOST_PATTERN.matcher(preparedHost).matches();
  }

  public String removeGreenInstancePrefix(final String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return host;
    }
    final Matcher matcher = BG_GREEN_HOST_PATTERN.matcher(getPreparedHost(host));
    if (!matcher.matches()) {
      return host;
    }
    final String prefix = matcher.group("prefix");
    if (StringUtils.isNullOrEmpty(prefix)) {
      return host;
    }
    return host.replace(prefix + ".", ".");
  }

  private Matcher cacheMatcher(final String host, Pattern... patterns) {
    Matcher matcher;
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
      final Matcher matcher = cacheMatcher(k,
          AURORA_DNS_PATTERN, AURORA_CHINA_DNS_PATTERN, AURORA_OLD_CHINA_DNS_PATTERN,
          AURORA_GOV_DNS_PATTERN, AURORA_GLOBAL_WRITER_DNS_PATTERN);
      return getRegexGroup(matcher, DNS_GROUP);
    });
  }

  public static void clearCache() {
    cachedPatterns.clear();
    cachedDnsPatterns.clear();
  }

  public static void setPrepareHostFunc(final Function<String, String> func) {
    prepareHostFunc.set(func);
  }

  public static void resetPrepareHostFunc() {
    prepareHostFunc.set(null);
  }

  private static String getPreparedHost(final String host) {
    final Function<String, String> func = prepareHostFunc.get();
    if (func == null) {
      return host;
    }
    final String preparedHost = func.apply(host);
    return preparedHost == null ? host : preparedHost;
  }
}
