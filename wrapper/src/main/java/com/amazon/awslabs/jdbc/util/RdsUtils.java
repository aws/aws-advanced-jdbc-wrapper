/*
 *
 *     Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License").
 *     You may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.amazon.awslabs.jdbc.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RdsUtils {

  public static final Pattern AURORA_DNS_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>proxy-|cluster-|cluster-ro-|cluster-custom-)?"
              + "(?<domain>[a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_CLUSTER_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>cluster-|cluster-ro-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern AURORA_CUSTOM_CLUSTER_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>cluster-custom-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern AURORA_PROXY_DNS_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>proxy-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
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

  public boolean isRdsClusterDns(String host) {
    return !StringUtils.isNullOrEmpty(host) && AURORA_CLUSTER_PATTERN.matcher(host).find();
  }

  public boolean isRdsCustomClusterDns(String host) {
    return !StringUtils.isNullOrEmpty(host) && AURORA_CUSTOM_CLUSTER_PATTERN.matcher(host).find();
  }

  public boolean isRdsDns(String host) {
    return !StringUtils.isNullOrEmpty(host) && AURORA_DNS_PATTERN.matcher(host).find();
  }

  public boolean isRdsProxyDns(String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return false;
    }
    return AURORA_PROXY_DNS_PATTERN.matcher(host).find();
  }

  public String getRdsInstanceHostPattern(String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return "?";
    }

    Matcher matcher = AURORA_DNS_PATTERN.matcher(host);
    if (matcher.find()) {
      return "?." + matcher.group(DOMAIN_GROUP);
    }
    return "?";
  }

  private boolean isWriterClusterDns(String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return false;
    }

    final Matcher matcher = AURORA_CLUSTER_PATTERN.matcher(host);
    if (matcher.find()) {
      return "cluster-".equalsIgnoreCase(matcher.group(DNS_GROUP));
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
    return null;
  }

  public boolean isIPv4(final String ip) {
    return !StringUtils.isNullOrEmpty(ip) && IP_V4.matcher(ip).matches();
  }

  public boolean isIPv6(final String ip) {
    return !StringUtils.isNullOrEmpty(ip) && IP_V6.matcher(ip).matches()
        || IP_V6_COMPRESSED.matcher(ip).matches();
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
