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

package software.amazon.jdbc.states;

import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An immutable snapshot of database session state that can affect authorization and object
 * resolution.
 *
 * <p>PostgreSQL exposes SET ROLE through {@code currentUser}. MySQL and MariaDB
 * keep {@code currentUser} unchanged and expose activated roles separately.
 * Therefore, both {@code currentUser} and {@code activeRoles} are required.
 */
public final class AuthorizationSessionState {

  private final String sessionUser;
  private final String currentUser;
  private final String searchPath;
  private final String resolvedSearchPath;
  // Active roles are separate from currentUser on MySQL and MariaDB.
  // Empty for dialects, such as PostgreSQL, that represent the effective role through currentUser.
  private final String activeRoles;
  private final String currentDatabase;

  public AuthorizationSessionState(
      final String sessionUser,
      final String currentUser,
      final String searchPath,
      final String resolvedSearchPath) {
    this(sessionUser, currentUser, searchPath, resolvedSearchPath, "", "");
  }

  public AuthorizationSessionState(
      final String sessionUser,
      final String currentUser,
      final String searchPath,
      final String resolvedSearchPath,
      final String activeRoles,
      final String currentDatabase) {
    this.sessionUser = Objects.requireNonNull(sessionUser);
    this.currentUser = Objects.requireNonNull(currentUser);
    this.searchPath = Objects.requireNonNull(searchPath);
    this.resolvedSearchPath = Objects.requireNonNull(resolvedSearchPath);
    this.activeRoles = Objects.requireNonNull(activeRoles);
    this.currentDatabase = Objects.requireNonNull(currentDatabase);
  }

  public String getSessionUser() {
    return this.sessionUser;
  }

  public String getCurrentUser() {
    return this.currentUser;
  }

  public String getSearchPath() {
    return this.searchPath;
  }

  public String getResolvedSearchPath() {
    return this.resolvedSearchPath;
  }

  public String getActiveRoles() {
    return this.activeRoles;
  }

  public String getCurrentDatabase() {
    return this.currentDatabase;
  }

  @Override
  public boolean equals(final @Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof AuthorizationSessionState)) {
      return false;
    }
    final AuthorizationSessionState that = (AuthorizationSessionState) other;
    return this.sessionUser.equals(that.sessionUser)
        && this.currentUser.equals(that.currentUser)
        && this.searchPath.equals(that.searchPath)
        && this.resolvedSearchPath.equals(that.resolvedSearchPath)
        && this.activeRoles.equals(that.activeRoles)
        && this.currentDatabase.equals(that.currentDatabase);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.sessionUser,
        this.currentUser,
        this.searchPath,
        this.resolvedSearchPath,
        this.activeRoles,
        this.currentDatabase);
  }
}
