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

import java.util.Map;
import java.util.Set;

/**
 * Standard context keys for SQL analysis results shared between plugins
 * via {@link software.amazon.jdbc.PluginCallContext}.
 */
public final class SqlContextKeys {

  private SqlContextKeys() {
  }

  /** Query type: SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, UNKNOWN. */
  public static final String QUERY_TYPE = "sql.queryType";

  /** Affected table names as {@code Set<String>}. */
  public static final String TABLES = "sql.tables";

  /** Parameter index to column name mapping as {@code Map<Integer, String>}. */
  public static final String PARAM_MAPPING = "sql.paramMapping";

  /** SQL with annotations stripped. */
  public static final String CLEAN_SQL = "sql.cleanSql";

  /** Annotation-based parameter overrides as {@code Map<Integer, String>}. */
  public static final String ANNOTATIONS = "sql.annotations";

  /** Routing hint from SQL comment: "reader", "writer", or null if not specified.
   *  Note: hints are parsed from SQL comments. Applications constructing SQL from
   *  user input should parameterize inputs to prevent hint injection. */
  public static final String ROUTING_HINT = "sql.routingHint";

  /** Whether the query contains a row-locking clause (FOR UPDATE, FOR SHARE, etc.). */
  public static final String FOR_UPDATE = "sql.forUpdate";
}
