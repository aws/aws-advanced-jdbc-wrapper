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

import java.util.Collection;
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;

public class Utils {
  public static boolean isNullOrEmpty(final Collection<?> c) {
    return c == null || c.isEmpty();
  }

  public static boolean containsUrl(final List<HostSpec> hosts, String url) {
    for (final HostSpec hostSpec : hosts) {
      if (hostSpec.getUrl().equals(url)) {
        return true;
      }
    }

    return false;
  }

  public static String logTopology(final @Nullable List<HostSpec> hosts) {
    return logTopology(hosts, null);
  }

  public static String logTopology(
      final @Nullable List<HostSpec> hosts,
      final @Nullable String messagePrefix) {

    final StringBuilder msg = new StringBuilder();
    if (hosts == null) {
      msg.append("<null>");
    } else {
      for (final HostSpec host : hosts) {
        if (msg.length() > 0) {
          msg.append("\n");
        }
        msg.append("   ").append(host == null ? "<null>" : host);
      }
    }

    return Messages.get("Utils.topology",
        new Object[] {messagePrefix == null ? "Topology:" : messagePrefix, msg.toString()});
  }
}
