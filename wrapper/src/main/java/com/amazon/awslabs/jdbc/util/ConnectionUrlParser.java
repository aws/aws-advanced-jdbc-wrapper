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

package com.amazon.awslabs.jdbc.util;

import com.amazon.awslabs.jdbc.HostSpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConnectionUrlParser {

  private static final String HOSTS_SEPARATOR = ",";
  static final String HOST_PORT_SEPARATOR = ":";
  static final Pattern CONNECTION_STRING_PATTERN =
      Pattern.compile(
          "(?<protocol>[\\w(\\-\\w)?\\+:%]+)\\s*" // Driver protocol. "word1:word2:..." or "word1-word2:word3:..."
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

  public static HostSpec parseHostPortPair(final String url) {
    final String[] hostPortPair = url.split(HOST_PORT_SEPARATOR, 2);
    if (hostPortPair.length > 1) {
      final String[] port = hostPortPair[1].split("/");
      int portValue = parsePortAsInt(hostPortPair[1]);
      if (port.length > 1) {
        portValue = parsePortAsInt(port[0]);
      }
      return new HostSpec(hostPortPair[0], portValue);
    }
    return new HostSpec(hostPortPair[0]);
  }

  private static int parsePortAsInt(String port) {
    try {
      return Integer.parseInt(port);
    } catch (NumberFormatException e) {
      return HostSpec.NO_PORT;
    }
  }

  // Get the database name from a given url of the generic format:
  // "protocol//[hosts][/database][?properties]"
  public static String parseDatabaseFromUrl(String url) {
    String[] dbName = url.split("//")[1].split("\\?")[0].split("/");

    if (dbName.length == 1) {
      return null;
    }

    return dbName[1];
  }

  // Get the user name from a given url of the generic format:
  // "protocol//[hosts][/database][?properties]"
  public static String parseUserFromUrl(String url, String userPropertyName) {
    String[] urlParameters = url.split("\\?");
    if (urlParameters.length == 1) {
      return null;
    }

    String[] user = urlParameters[1].split(userPropertyName + "=")[1].split("&");
    if (StringUtils.isNullOrEmpty(user[0])) {
      return null;
    }

    return user[0];
  }

  // Get the password from a given url of the generic format:
  // "protocol//[hosts][/database][?properties]"
  public static String parsePasswordFromUrl(String url, String passwordPropertyName) {
    String[] urlParameters = url.split("\\?");
    if (urlParameters.length == 1) {
      return null;
    }

    String[] password = urlParameters[1].split(passwordPropertyName + "=")[1].split("&");
    if (StringUtils.isNullOrEmpty(password[0])) {
      return null;
    }

    return password[0];
  }

  // Get the properties from a given url of the generic format:
  // "protocol//[hosts][/database][?properties]"
  public static Properties parsePropertiesFromUrl(String url, Properties props) {
    String[] urlParameters = url.split("\\?");
    if (urlParameters.length == 1) {
      return null;
    }

    String[] listOfParameters = urlParameters[1].split("&");
    for (String param : listOfParameters) {
      String[] currentParameter = param.split("=");
      String currentParameterName = currentParameter[0];
      String currentParameterValue = "";

      if (currentParameter.length > 1) {
        currentParameterValue = currentParameter[1];
      }

      props.setProperty(currentParameterName, currentParameterValue);
    }

    return props;
  }
}
