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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;

public class ConnectionUrlParser {

  private static final Logger LOGGER = Logger.getLogger(ConnectionUrlParser.class.getName());
  private static final String HOSTS_SEPARATOR = ",";
  static final String HOST_PORT_SEPARATOR = ":";
  static final Pattern CONNECTION_STRING_PATTERN =
      Pattern.compile(
          "(?<protocol>[\\w(\\-)?+:%]+)\\s*" // Driver protocol. "word1:word2:..." or "word1-word2:word3:..."
              + "(?://(?<hosts>[^/?#]*))?\\s*" // Optional list of host(s) starting with // and
              // follows by any char except "/", "?" or "#"
              + "(?:[/?#].*)?"); // Anything starting with either "/", "?" or "#"

  private static final Pattern URL_WITH_REGION_PATTERN =
      Pattern.compile(
          "^(\\[(?<region>.+)\\])?(?<domain>[a-zA-Z0-9\\?\\.\\-]+)(:(?<port>[0-9]+))?$",
          Pattern.CASE_INSENSITIVE);

  static final Pattern EMPTY_STRING_IN_QUOTATIONS = Pattern.compile("\"(\\s*)\"");
  private static final RdsUtils rdsUtils = new RdsUtils();

  public List<HostSpec> getHostsFromConnectionUrl(final String initialConnection,
                                                  final boolean singleWriterConnectionString,
                                                  final Supplier<HostSpecBuilder> hostSpecBuilderSupplier) {
    final List<HostSpec> hostsList = new ArrayList<>();
    final Matcher matcher = CONNECTION_STRING_PATTERN.matcher(initialConnection);
    if (!matcher.matches()) {
      return hostsList;
    }

    final String hosts = matcher.group("hosts") == null ? null : matcher.group("hosts").trim();
    if (hosts != null) {
      final String[] hostArray = hosts.split(HOSTS_SEPARATOR);
      for (int i = 0; i < hostArray.length; i++) {
        final HostSpec host;
        if (singleWriterConnectionString) {
          final HostRole role = i > 0 ? HostRole.READER : HostRole.WRITER;
          host = parseHostPortPair(hostArray[i], role, hostSpecBuilderSupplier);
        } else {
          host = parseHostPortPair(hostArray[i], hostSpecBuilderSupplier);
        }

        if (!StringUtils.isNullOrEmpty(host.getHost())) {
          hostsList.add(host);
        }
      }
    }

    return hostsList;
  }

  public static HostSpec parseHostPortPair(final String url, final Supplier<HostSpecBuilder> hostSpecBuilderSupplier) {
    final String[] hostPortPair = url.split(HOST_PORT_SEPARATOR, 2);
    final RdsUrlType urlType = rdsUtils.identifyRdsType(hostPortPair[0]);
    // Assign HostRole of READER if using the reader cluster URL, otherwise assume a HostRole of WRITER
    final HostRole hostRole = RdsUrlType.RDS_READER_CLUSTER.equals(urlType) ? HostRole.READER : HostRole.WRITER;
    return getHostSpec(hostPortPair, hostRole, hostSpecBuilderSupplier.get());
  }

  public static HostSpec parseHostPortPair(final String url, final HostRole role,
      final Supplier<HostSpecBuilder> hostSpecBuilderSupplier) {
    final String[] hostPortPair = url.split(HOST_PORT_SEPARATOR, 2);
    return getHostSpec(hostPortPair, role, hostSpecBuilderSupplier.get());
  }

  /**
   * Parse strings in the following formats:
   * "url", for example: "instance-1.XYZ.us-east-2.rds.amazonaws.com"
   * "url:port", for example: "instance-1.XYZ.us-east-2.rds.amazonaws.com:9999"
   * "[region_name]url", for example: "us-east-2:instance-1.any-domain.com"
   * "[region_name]url:port", for example: "us-east-2:instance-1.any-domain.com:9999"
   */
  public static Pair<String, HostSpec> parseHostPortPairWithRegionPrefix(
      final String urlWithRegionPrefix,
      final Supplier<HostSpecBuilder> hostSpecBuilderSupplier) {

    final Matcher matcher = URL_WITH_REGION_PATTERN.matcher(urlWithRegionPrefix);
    if (!matcher.find()) {
      throw new IllegalArgumentException(String.format("Can't recognize AWS region in '%s'", urlWithRegionPrefix));
    }
    String awsRegion = matcher.group("region");
    final String host = matcher.group("domain");
    final String port = matcher.group("port");

    if (StringUtils.isNullOrEmpty(host)) {
      throw new IllegalArgumentException(String.format("Can't recognize host in '%s'", urlWithRegionPrefix));
    }

    if (StringUtils.isNullOrEmpty(awsRegion)) {
      awsRegion = rdsUtils.getRdsRegion(host);
      if (StringUtils.isNullOrEmpty(awsRegion)) {
        throw new IllegalArgumentException(String.format("Can't recognize AWS region in '%s'", urlWithRegionPrefix));
      }
    }

    final RdsUrlType urlType = rdsUtils.identifyRdsType(host);

    // Assign HostRole of READER if using the reader cluster URL, otherwise assume a HostRole of WRITER
    final HostRole hostRole = RdsUrlType.RDS_READER_CLUSTER.equals(urlType) ? HostRole.READER : HostRole.WRITER;
    final String[] hostPortPair = StringUtils.isNullOrEmpty(port)
        ? new String[] { host }
        : new String[] { host, port };
    return Pair.create(awsRegion, getHostSpec(hostPortPair, hostRole, hostSpecBuilderSupplier.get()));
  }

  private static HostSpec getHostSpec(final String[] hostPortPair, final HostRole hostRole,
      final HostSpecBuilder hostSpecBuilder) {
    String hostId = rdsUtils.getRdsInstanceId(hostPortPair[0]);

    if (hostPortPair.length > 1) {
      final String[] port = hostPortPair[1].split("/");
      int portValue = parsePortAsInt(hostPortPair[1]);
      if (port.length > 1) {
        portValue = parsePortAsInt(port[0]);
      }
      return hostSpecBuilder
          .host(hostPortPair[0])
          .port(portValue)
          .hostId(hostId)
          .role(hostRole)
          .build();
    }
    return hostSpecBuilder
        .host(hostPortPair[0])
        .port(HostSpec.NO_PORT)
        .hostId(hostId)
        .role(hostRole)
        .build();
  }

  private static int parsePortAsInt(final String port) {
    try {
      return Integer.parseInt(port);
    } catch (final NumberFormatException e) {
      return HostSpec.NO_PORT;
    }
  }

  // Get the database name from a given url of the generic format:
  // "protocol//[hosts][/database][?properties]"
  public static String parseDatabaseFromUrl(final String url) {
    final String[] dbName = url.split("//")[1].split("\\?")[0].split("/");

    if (dbName.length == 1) {
      return null;
    }

    return dbName[1];
  }

  // Get the user name from a given url of the generic format:
  // "protocol//[hosts][/database][?properties]"
  public static String parseUserFromUrl(final String url) {
    final Pattern userPattern = Pattern.compile("user=(?<username>[^&]*)");
    final Matcher matcher = userPattern.matcher(url);
    if (matcher.find()) {
      return matcher.group("username");
    }

    return null;
  }

  // Get the password from a given url of the generic format:
  // "protocol//[hosts][/database][?properties]"
  public static String parsePasswordFromUrl(final String url) {
    final Pattern passwordPattern = Pattern.compile("password=(?<pass>[^&]*)");
    final Matcher matcher = passwordPattern.matcher(url);
    if (matcher.find()) {
      return matcher.group("pass");
    }

    return null;
  }

  // Get the properties from a given url of the generic format:
  // "protocol//[hosts][/database][?properties]"
  public static void parsePropertiesFromUrl(final String url, final Properties props) {
    String[] urlParameters = url.split("\\?", 2);
    if (urlParameters.length == 1) {
      return;
    }

    final String[] listOfParameters = urlParameters[1].split("&");
    for (final String param : listOfParameters) {
      final int pos = param.indexOf("=");
      String currentParameterValue = "";

      if (pos == -1) {
        props.setProperty(param, currentParameterValue);
        continue;
      }

      currentParameterValue = urlDecode(param.substring(pos + 1));
      if (currentParameterValue == null) {
        continue;
      }

      // Special handling for empty parameters in the form of [param=\"\"]
      // Empty parameters would have extra quotations after splitting, i.e. [param, """"], instead of [param, ""]
      final Matcher matcher = EMPTY_STRING_IN_QUOTATIONS.matcher(currentParameterValue);
      if (matcher.matches()) {
        currentParameterValue = "";
      }
      final String currentParameterName = param.substring(0, pos);
      props.setProperty(currentParameterName, currentParameterValue);
    }
  }

  private static @Nullable String urlDecode(final String url) {
    try {
      return StringUtils.decode(url);
    } catch (final IllegalArgumentException e) {
      LOGGER.fine(
          () -> Messages.get(
              "Driver.urlParsingFailed",
              new Object[] {url, e.getMessage()}));
    }

    // Attempt to use the original value for connection.
    return url;
  }

  public String getProtocol(final String url) {
    final int index = url.indexOf("//");
    if (index < 0) {
      throw new IllegalArgumentException(
          Messages.get(
              "ConnectionUrlParser.protocolNotFound",
              new Object[] {url}));
    }
    return url.substring(0, index + 2);
  }
}
