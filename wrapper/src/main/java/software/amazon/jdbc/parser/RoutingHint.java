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

package software.amazon.jdbc.parser;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Explicit routing hint parsed from a SQL comment (e.g. {@code /*@reader* /},
 * {@code /*@writer* /}, or {@code /*@keep* /}). Used to override the query-type-based
 * routing decision: {@link #READER} and {@link #WRITER} force a specific connection role,
 * while {@link #KEEP} suppresses re-routing and runs the statement on the current connection.
 */
public enum RoutingHint {
  READER,
  WRITER,
  KEEP;

  /**
   * Parses a routing hint keyword into the corresponding enum value.
   *
   * @param value the hint keyword (case-insensitive), e.g. "reader", "writer", or "keep"
   * @return the matching {@link RoutingHint}, or null if the value is null or unrecognized
   */
  public static @Nullable RoutingHint fromString(final @Nullable String value) {
    if (value == null) {
      return null;
    }
    switch (value.toLowerCase()) {
      case "reader":
        return READER;
      case "writer":
        return WRITER;
      case "keep":
        return KEEP;
      default:
        return null;
    }
  }
}
