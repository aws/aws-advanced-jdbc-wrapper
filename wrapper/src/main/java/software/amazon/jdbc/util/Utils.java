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
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;

public class Utils {
  public static boolean isNullOrEmpty(final Collection<?> c) {
    return c == null || c.isEmpty();
  }

  public static boolean containsHostAndPort(final Collection<HostSpec> hosts, String hostAndPort) {
    if (Utils.isNullOrEmpty(hosts)) {
      return false;
    }

    for (final HostSpec hostSpec : hosts) {
      if (hostSpec.getHostAndPort().equals(hostAndPort)) {
        return true;
      }
    }

    return false;
  }

  public static @Nullable HostSpec getWriter(final List<HostSpec> topology) {
    if (topology == null || topology.isEmpty()) {
      return null;
    }

    for (final HostSpec host : topology) {
      if (host.getRole() == HostRole.WRITER) {
        return host;
      }
    }
    return null;
  }
}
