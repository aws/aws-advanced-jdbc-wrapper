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

package software.amazon.jdbc.hostlistprovider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.Nullable;

public enum MonitoringConnectionPriority {
  STRICT_WRITER,
  STRICT_READER,
  WRITER_OR_READER;

  private static final Map<String, MonitoringConnectionPriority> nameToValue =
      new HashMap<String, MonitoringConnectionPriority>() {
        {
          put("strict-writer", STRICT_WRITER);
          put("strict-reader", STRICT_READER);
          put("writer-or-reader", WRITER_OR_READER);
        }
      };

  public static @Nullable MonitoringConnectionPriority fromValue(final @Nullable String value) {
    if (value == null) {
      return null;
    }
    return nameToValue.get(value.trim().toLowerCase());
  }

  public static List<MonitoringConnectionPriority> parseList(final @Nullable String value) {
    List<MonitoringConnectionPriority> result = new ArrayList<>();
    if (value == null || value.trim().isEmpty()) {
      result.add(STRICT_WRITER);
      return result;
    }
    for (String item : value.split(",")) {
      MonitoringConnectionPriority priority = fromValue(item.trim());
      if (priority != null && !result.contains(priority)) {
        result.add(priority);
      }
    }
    if (result.isEmpty()) {
      result.add(STRICT_WRITER);
    }
    return result;
  }

  /**
   * Checks if a connection with the given writer status satisfies this priority.
   *
   * @param isWriter true if the connection is to a writer instance
   * @return true if the connection satisfies this priority requirement
   */
  public boolean isSatisfiedBy(boolean isWriter) {
    switch (this) {
      case STRICT_WRITER:
        return isWriter;
      case STRICT_READER:
        return !isWriter;
      case WRITER_OR_READER:
        return true;
      default:
        return false;
    }
  }
}
