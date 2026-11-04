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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import software.amazon.jdbc.util.Messages;

/** Builds the networked and offline Gradle preparation steps for the Hibernate image. */
public final class HibernateGradleRuntime {

  private static final String RESOLVE_RUNTIME_INIT =
      "import org.gradle.api.tasks.testing.Test\n"
          + "gradle.taskGraph.whenReady { graph ->\n"
          + "  graph.allTasks.findAll { it instanceof Test }.each { task ->\n"
          + "    println(\"Resolving runtime classpath for ${task.path}\")\n"
          + "    task.classpath.files\n"
          + "  }\n"
          + "}\n";

  private HibernateGradleRuntime() {}

  /** Returns the init script that resolves each Test task's complete runtime classpath. */
  public static String runtimeResolutionInitScript() {
    return RESOLVE_RUNTIME_INIT;
  }

  /** Returns Dockerfile instructions that populate Hibernate's image-owned Gradle cache. */
  public static List<String> buildSteps(
      final Path files, final String hibernateDirectory, final String hibernateVersion) {
    return Arrays.asList(
        "RUN apk add --no-cache --upgrade bash git",
        "ENV GRADLE_USER_HOME=" + GradleRuntime.CONTAINER_USER_HOME,
        "RUN git clone --depth 1 --branch " + hibernateVersion
            + " https://github.com/hibernate/hibernate-orm.git " + hibernateDirectory,
        buildFileStep(files.resolve("gradle.properties"), hibernateDirectory + "/gradle.properties"),
        buildFileStep(files.resolve("settings.gradle"), hibernateDirectory + "/settings.gradle"),
        buildFileStep(
            files.resolve("local.databases.gradle"),
            hibernateDirectory + "/local-build-plugins/src/main/groovy/local.databases.gradle"),
        buildTextStep(RESOLVE_RUNTIME_INIT, "/tmp/orchestra-resolve-runtime.gradle"),
        "RUN cd " + hibernateDirectory
            + " && ./gradlew --no-daemon --no-parallel --console=plain -Pdb=pg_amazon_ci"
            + " -I /tmp/orchestra-resolve-runtime.gradle test --dry-run"
            + " && ./gradlew --no-daemon --no-parallel --console=plain --offline"
            + " -Pdb=pg_amazon_ci test --dry-run");
  }

  private static String buildFileStep(final Path source, final String destination) {
    try {
      return buildTextStep(
          new String(Files.readAllBytes(source), StandardCharsets.UTF_8), destination);
    } catch (final IOException e) {
      throw new IllegalStateException(Messages.get(
          "HibernateGradleRuntime.buildInputReadFailed",
          new Object[] {source.toAbsolutePath()}), e);
    }
  }

  private static String buildTextStep(final String contents, final String destination) {
    final String encoded = Base64.getEncoder().encodeToString(
        contents.getBytes(StandardCharsets.UTF_8));
    return "RUN printf '%s' '" + encoded + "' | base64 -d > " + destination;
  }
}
