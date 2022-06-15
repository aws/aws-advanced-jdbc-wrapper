/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import software.aws.rds.jdbc.proxydriver.HostSpec;

public class ConnectionUrlParser {

  private static final String HOSTS_SEPARATOR = ",";
  private static final String HOST_PORT_SEPARATOR = ":";
  static final Pattern CONNECTION_STRING_PATTERN =
      Pattern.compile(
          "(?<protocol>[\\w\\+:%]+)\\s*" // Driver protocol
              + "(?://(?<hosts>[^/?#]*))?\\s*" // Optional list of host(s) starting with // and
              // follows by any char except "/", "?" or "#"
              + "(?:[/?#].*)?"); // Anything starting with either "/", "?" or "#"

  public List<HostSpec> getHostsFromConnectionUrl(final String initialConnection) {
    final List<HostSpec> hostsList = new ArrayList<>();

    final Matcher matcher = CONNECTION_STRING_PATTERN.matcher(initialConnection);
    if (!matcher.matches()) {
      return hostsList;
    }
    final String hosts = matcher.group("hosts") == null ? null : matcher.group("hosts").trim();
    if (hosts != null) {
      Arrays
          .stream(hosts.split(HOSTS_SEPARATOR))
          .forEach(hostString -> {
            final String[] hostPortPair = hostString.split(HOST_PORT_SEPARATOR, 2);
            final String host = hostPortPair[0];
            if (host.isEmpty()) {
              return;
            }
            if (hostPortPair.length > 1 && isNumeric(hostPortPair[1])) {
              hostsList.add(new HostSpec(host, Integer.parseInt(hostPortPair[1])));
            } else {
              hostsList.add(new HostSpec(host));
            }
          });
    }

    return hostsList;
  }

  private boolean isNumeric(String num) {
    try {
      Integer.parseInt(num);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
