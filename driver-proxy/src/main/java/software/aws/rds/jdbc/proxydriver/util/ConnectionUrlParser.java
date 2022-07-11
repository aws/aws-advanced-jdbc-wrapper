/*
*    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* 
*    Licensed under the Apache License, Version 2.0 (the "License").
*    You may not use this file except in compliance with the License.
*    You may obtain a copy of the License at
* 
*    http://www.apache.org/licenses/LICENSE-2.0
* 
*    Unless required by applicable law or agreed to in writing, software
*    distributed under the License is distributed on an "AS IS" BASIS,
*    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*    See the License for the specific language governing permissions and
*    limitations under the License.
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
  static final String HOST_PORT_SEPARATOR = ":";
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
            final HostSpec host = parseHostPortPair(hostString);
            if (host.getHost().isEmpty()) {
              return;
            }
            hostsList.add(host);
          });
    }

    return hostsList;
  }

  HostSpec parseHostPortPair(final String url) {
    final String[] hostPortPair = url.split(HOST_PORT_SEPARATOR, 2);
    if (hostPortPair.length > 1) {
      return new HostSpec(hostPortPair[0], parsePortAsInt(hostPortPair[1]));
    }
    return new HostSpec(hostPortPair[0]);
  }

  private int parsePortAsInt(String port) {
    try {
      return Integer.parseInt(port);
    } catch (NumberFormatException e) {
      return HostSpec.NO_PORT;
    }
  }
}
