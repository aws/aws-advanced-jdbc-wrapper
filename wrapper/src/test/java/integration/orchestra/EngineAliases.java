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

package integration.orchestra;

import integration.DatabaseEngine;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import software.amazon.jdbc.util.TestMessages;

/** Normalizes database-engine tokens shared by the Docker and RDS Orchestra tasks. */
public final class EngineAliases {

  private EngineAliases() {
  }

  /**
   * Parses an engine list. PostgreSQL accepts {@code pg}, {@code postgres}, and {@code postgresql};
   * MySQL accepts {@code mysql}; MariaDB accepts {@code mariadb} when the caller can provision it.
   */
  public static DatabaseEngine[] requested(
      final String requested,
      final DatabaseEngine defaultEngine,
      final boolean mariaDbSupported) {

    if (requested == null || requested.trim().isEmpty()) {
      return new DatabaseEngine[] {defaultEngine};
    }

    final List<DatabaseEngine> engines = new ArrayList<>();
    for (final String name : requested.split(",")) {
      final String token = name.trim().toLowerCase(Locale.ROOT);
      if (token.isEmpty()) {
        continue;
      }

      switch (token) {
        case "pg":
        case "postgres":
        case "postgresql":
          engines.add(DatabaseEngine.PG);
          break;
        case "mysql":
          engines.add(DatabaseEngine.MYSQL);
          break;
        case "mariadb":
          if (!mariaDbSupported) {
            throw new IllegalArgumentException(TestMessages.get(
                "EngineAliases.mariaDbUnsupported"));
          }
          engines.add(DatabaseEngine.MARIADB);
          break;
        default:
          throw new IllegalArgumentException(TestMessages.get(
              "EngineAliases.unsupported", new Object[] {token,
                  mariaDbSupported ? "pg, postgres, postgresql, mysql, mariadb"
                      : "pg, postgres, postgresql, mysql"}));
      }
    }

    if (engines.isEmpty()) {
      throw new IllegalArgumentException(TestMessages.get("EngineAliases.empty"));
    }
    return engines.toArray(new DatabaseEngine[0]);
  }
}
