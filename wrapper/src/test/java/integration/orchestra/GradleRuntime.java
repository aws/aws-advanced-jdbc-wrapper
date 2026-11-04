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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import software.amazon.jdbc.util.Messages;

/** Supplies isolated Gradle paths to Orchestra test containers. */
public final class GradleRuntime {

  public static final String INSTALLATION_PROPERTY = "orchestra-gradle-home";
  public static final String CACHE_DIRECTORY_PROPERTY = "orchestra-gradle-cache-directory";
  public static final String MODULE_CACHE_ARCHIVE_PROPERTY = "orchestra-gradle-module-cache-archive";
  public static final String CONTAINER_BUILD_FILE_PROPERTY = "orchestra-gradle-container-build-file";

  static final String CONTAINER_INSTALLATION = "/opt/orchestra-gradle";
  static final String CONTAINER_CACHE_OUTPUT = "/opt/orchestra-cache-output";
  static final String CONTAINER_BUILD_FILE = "/app/build.gradle.kts";
  static final String CONTAINER_LAUNCHER = "/opt/orchestra-gradle-launcher";
  static final String CONTAINER_MODULE_CACHE_ARCHIVE = "/opt/orchestra-gradle-modules.jar";
  static final String CONTAINER_USER_HOME = "/opt/orchestra-gradle-user-home";

  private GradleRuntime() {}

  /** Returns the host Gradle installation mounted into each test container. */
  public static Map<Path, String> containerBinds() {
    final Map<Path, String> binds = new LinkedHashMap<>();
    binds.put(requiredDirectory(INSTALLATION_PROPERTY), CONTAINER_INSTALLATION);
    return binds;
  }

  /** Returns the isolated module-cache archive copied onto the container filesystem. */
  public static Map<Path, String> containerCopies() {
    final Path archive = requiredFile(MODULE_CACHE_ARCHIVE_PROPERTY);
    final Map<Path, String> copies = new LinkedHashMap<>();
    copies.put(archive, CONTAINER_MODULE_CACHE_ARCHIVE);
    return copies;
  }

  /** Returns the Gradle environment shared by standard and Hibernate containers. */
  public static Map<String, String> containerEnvironment() {
    final Map<String, String> environment = new LinkedHashMap<>();
    environment.put("GRADLE_USER_HOME", CONTAINER_USER_HOME);
    return environment;
  }

  /** Returns the xargs-free Gradle launcher installed in each final test container. */
  public static String containerCommand() {
    return CONTAINER_LAUNCHER;
  }

  /** Returns the standard Gradle launcher used by the Alpine cache resolver. */
  static String resolverCommand() {
    return CONTAINER_INSTALLATION + "/bin/gradle";
  }

  /** Returns arguments that prohibit final test containers from consulting external repositories. */
  public static List<String> offlineArguments() {
    return Arrays.asList("--no-parallel", "--no-daemon", "--offline");
  }

  /** Returns the command that installs the prepared cache and xargs-free Gradle launcher. */
  public static String cacheExtractionCommand() {
    return "mkdir -p " + CONTAINER_USER_HOME
        + " && cd " + CONTAINER_USER_HOME
        + " && jar -xf " + CONTAINER_MODULE_CACHE_ARCHIVE
        + " && printf '%s\\n' '#!/bin/sh' 'exec java -jar " + CONTAINER_INSTALLATION
        + "/lib/gradle-gradle-cli-main-*.jar \"$@\"' > " + CONTAINER_LAUNCHER
        + " && chmod 755 " + CONTAINER_LAUNCHER
        + " && rm -f " + CONTAINER_MODULE_CACHE_ARCHIVE;
  }

  static Path requiredDirectory(final String property) {
    final Path path = configuredPath(property);
    if (!Files.isDirectory(path)) {
      throw new IllegalStateException(Messages.get(
          "GradleRuntime.directoryMissing", new Object[] {property, path}));
    }
    return path;
  }

  static Path requiredFile(final String property) {
    final Path path = configuredPath(property);
    if (!Files.isRegularFile(path)) {
      throw new IllegalStateException(Messages.get(
          "GradleRuntime.fileMissing", new Object[] {property, path}));
    }
    return path;
  }

  static Path configuredPath(final String property) {
    return Paths.get(requiredProperty(property)).toAbsolutePath().normalize();
  }

  private static String requiredProperty(final String property) {
    final String configured = System.getProperty(property);
    if (configured == null || configured.trim().isEmpty()) {
      throw new IllegalStateException(Messages.get(
          "GradleRuntime.propertyUnset", new Object[] {property}));
    }
    return configured.trim();
  }
}
