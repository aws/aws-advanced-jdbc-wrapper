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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import software.amazon.jdbc.util.Messages;
import software.amazon.orchestra.instruments.docker.JavaTestContainerConfiguration;

/** Builds the isolated module cache used by standard Docker and RDS test containers. */
final class PrepareGradleCache {

  private static final Logger LOGGER = Logger.getLogger(PrepareGradleCache.class.getName());

  private PrepareGradleCache() {}

  /**
   * Resolves the copied test build in an Alpine container with normal egress, then closes and archives its
   * dedicated module cache for the final no-egress containers.
   */
  static void execute(final JavaTestContainerConfiguration configuration) throws Exception {
    final Path installation = GradleRuntime.requiredDirectory(GradleRuntime.INSTALLATION_PROPERTY);
    final Path cacheDirectory = requiredEmptyDirectory(GradleRuntime.CACHE_DIRECTORY_PROPERTY);
    final Path archive = GradleRuntime.configuredPath(GradleRuntime.MODULE_CACHE_ARCHIVE_PROPERTY);
    final Path buildFile = GradleRuntime.requiredFile(GradleRuntime.CONTAINER_BUILD_FILE_PROPERTY);
    final Path archiveDirectory = archive.getParent();
    if (archiveDirectory == null) {
      throw new IllegalStateException(Messages.get(
          "GradleRuntime.archiveParentMissing", new Object[] {archive}));
    }
    Files.createDirectories(archiveDirectory);
    Files.deleteIfExists(archive);

    final ArchiveContents contents;
    try (GenericContainer<?> resolver =
        new GenericContainer<>(DockerImageName.parse(configuration.getTestContainerImage()))) {
      resolver
          .withWorkingDirectory("/app")
          .withCommand("sleep", "infinity")
          .withEnv(GradleRuntime.containerEnvironment())
          .withFileSystemBind(
              installation.toString(), GradleRuntime.CONTAINER_INSTALLATION, BindMode.READ_ONLY)
          .withFileSystemBind(
              cacheDirectory.toString(), GradleRuntime.CONTAINER_USER_HOME, BindMode.READ_WRITE)
          .withFileSystemBind(
              archiveDirectory.toString(), GradleRuntime.CONTAINER_CACHE_OUTPUT, BindMode.READ_WRITE)
          .withCopyFileToContainer(
              MountableFile.forHostPath(buildFile), GradleRuntime.CONTAINER_BUILD_FILE);

      // No Orchestra network is assigned here. Testcontainers' ordinary bridge gives this short-lived
      // resolver egress, while the final suite container remains on Orchestra's internal client network.
      resolver.start();

      executeCommand(resolver, "GradleRuntime.resolveFailed",
          GradleRuntime.resolverCommand(), "resolveOrchestraRuntime", "--no-daemon", "--no-parallel",
          "--console=plain");
      validateValkeyClassifiers(resolver);

      // The resolver command above has exited and --no-daemon leaves no Gradle writer behind. Validate the
      // final task with the same build and user home before creating a lock-free archive.
      final Container.ExecResult offline = executeCommand(
          resolver,
          "GradleRuntime.offlineValidationFailed",
          GradleRuntime.resolverCommand(),
          "in-container",
          "--offline",
          "--dry-run",
          "--no-daemon",
          "--no-parallel",
          "--console=plain");
      rejectDownloadAttempt(offline);

      final String archiveCommand = "find " + GradleRuntime.CONTAINER_USER_HOME
          + "/caches/modules-2 -type f \\( -name '*.lock' -o -name gc.properties \\) -delete"
          + " && jar -cfM " + GradleRuntime.CONTAINER_CACHE_OUTPUT + "/" + archive.getFileName()
          + " -C " + GradleRuntime.CONTAINER_USER_HOME + " caches/modules-2";
      executeCommand(resolver, "GradleRuntime.archiveFailed", "sh", "-c", archiveCommand);
      final Container.ExecResult listing = executeCommand(
          resolver,
          "GradleRuntime.archiveListFailed",
          "jar",
          "-tf",
          GradleRuntime.CONTAINER_CACHE_OUTPUT + "/" + archive.getFileName());
      contents = parseContents(listing.getStdout());
    }

    if (!Files.isRegularFile(archive)) {
      throw new IllegalStateException(Messages.get(
          "GradleRuntime.archiveMissing", new Object[] {archive}));
    }
    final long size = Files.size(archive);
    LOGGER.info(() -> Messages.get(
        "GradleRuntime.prepared",
        new Object[] {archive, size, contents.entryCount, contents.coordinates}));
  }

  private static void validateValkeyClassifiers(final GenericContainer<?> resolver) throws Exception {
    final String moduleDirectory = GradleRuntime.CONTAINER_USER_HOME
        + "/caches/modules-2/files-2.1/io.valkey/valkey-glide/2.3.0";
    final String command = "case \"$(uname -m)\" in aarch64|arm64) arch=aarch_64 ;; "
        + "*) arch=x86_64 ;; esac"
        + " && test -n \"$(find " + moduleDirectory
        + " -type f -name \"valkey-glide-2.3.0-linux-$arch.jar\" -print -quit)\""
        + " && test -n \"$(find " + moduleDirectory
        + " -type f -name \"valkey-glide-2.3.0-linux_musl-$arch.jar\" -print -quit)\"";
    executeCommand(resolver, "GradleRuntime.valkeyClassifiersMissing", "sh", "-c", command);
    LOGGER.info(() -> Messages.get("GradleRuntime.valkeyClassifiersValidated"));
  }

  private static Path requiredEmptyDirectory(final String property) throws IOException {
    final Path path = GradleRuntime.configuredPath(property);
    Files.createDirectories(path);
    try (Stream<Path> entries = Files.list(path)) {
      if (entries.findAny().isPresent()) {
        throw new IllegalStateException(Messages.get(
            "GradleRuntime.cacheNotEmpty", new Object[] {property, path}));
      }
    }
    return path;
  }

  private static Container.ExecResult executeCommand(
      final GenericContainer<?> container, final String messageKey, final String... command)
      throws Exception {
    final Container.ExecResult result = container.execInContainer(command);
    if (result.getExitCode() != 0) {
      throw new IllegalStateException(Messages.get(
          messageKey,
          new Object[] {result.getExitCode(), result.getStdout(), result.getStderr()}));
    }
    return result;
  }

  private static void rejectDownloadAttempt(final Container.ExecResult result) {
    final String output = (result.getStdout() + '\n' + result.getStderr()).toLowerCase(Locale.ROOT);
    if (output.contains("services.gradle.org")
        || output.contains("repo.maven")
        || output.contains("plugins.gradle.org")
        || output.contains("downloading ")) {
      throw new IllegalStateException(Messages.get(
          "GradleRuntime.offlineDownloadAttempt", new Object[] {result.getStdout(), result.getStderr()}));
    }
    LOGGER.info(() -> Messages.get("GradleRuntime.offlineValidated"));
  }

  private static ArchiveContents parseContents(final String listing) {
    final String[] entries = listing.split("\\R");
    final Set<String> coordinates = new LinkedHashSet<>();
    for (final String entry : entries) {
      final String prefix = "caches/modules-2/files-2.1/";
      if (!entry.startsWith(prefix)) {
        continue;
      }
      final String[] segments = entry.substring(prefix.length()).split("/");
      if (segments.length >= 3) {
        coordinates.add(segments[0] + ":" + segments[1] + ":" + segments[2]);
      }
    }
    return new ArchiveContents(entries.length, coordinates);
  }

  private static final class ArchiveContents {
    private final int entryCount;
    private final Set<String> coordinates;

    private ArchiveContents(final int entryCount, final Set<String> coordinates) {
      this.entryCount = entryCount;
      this.coordinates = coordinates;
    }
  }
}
