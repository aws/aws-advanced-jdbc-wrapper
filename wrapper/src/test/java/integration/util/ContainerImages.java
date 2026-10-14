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

package integration.util;

import integration.TargetJvm;
import java.util.Locale;
import java.util.logging.Logger;
import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;
import org.testcontainers.utility.DockerImageName;
import software.amazon.jdbc.util.StringUtils;

/**
 * Single source for the container images the integration tests pull, so a run can take them either
 * from Docker Hub or from Amazon ECR Public.
 *
 * <p>Docker Hub rate limits anonymous pulls per source IP. A runner that shares its IP with others,
 * or any run that has no Docker Hub account to authenticate with, hits that limit and fails while
 * pulling. Such a run selects the ECR Public mirrors instead, which need no credentials:
 *
 * <pre>
 *   ./gradlew test-all-pg-aurora -Dtest-container-registry=ecr
 *   TEST_CONTAINER_REGISTRY=ecr ./gradlew test-all-pg-aurora
 * </pre>
 *
 * <p>The system property wins over the environment variable; the default is {@code dockerhub}, which
 * keeps every image exactly where it was before this setting existed.
 *
 * <p>A few images are served from registries that have no ECR Public mirror and are unaffected by
 * this setting: toxiproxy and GraalVM come from GitHub's registry, which does not apply Docker Hub's
 * limits, and postgis is only used by the local Docker test environments.
 */
public final class ContainerImages {

  private static final Logger LOGGER = Logger.getLogger(ContainerImages.class.getName());

  private static final String DOCKER_HUB = "dockerhub";
  private static final String ECR = "ecr";

  /** ECR Public mirror of the Docker Official Images, tag-for-tag identical to Docker Hub. */
  private static final String ECR_DOCKER_LIBRARY = "public.ecr.aws/docker/library/";

  private static final boolean USE_ECR = resolveUseEcr();

  private ContainerImages() {
  }

  /** Base image of the container the integration tests themselves run in. */
  public static String jvmBase(TargetJvm jvm) {
    switch (jvm) {
      case OPENJDK8:
        return dockerOfficialImage("amazoncorretto", "8-alpine");
      case OPENJDK11:
        return dockerOfficialImage("amazoncorretto", "11.0.19-alpine3.17");
      case OPENJDK17:
        return dockerOfficialImage("amazoncorretto", "17-alpine3.21");
      case OPENJDK21:
        return dockerOfficialImage("amazoncorretto", "21-alpine-full");
      case OPENJDK24:
        return dockerOfficialImage("amazoncorretto", "24-alpine-full");
      case GRAALVM:
        return "ghcr.io/graalvm/jdk:22.2.0";
      default:
        throw new NotImplementedException(jvm.toString());
    }
  }

  public static DockerImageName mysql() {
    return dockerOfficial("mysql", "8.0.31");
  }

  public static DockerImageName postgres() {
    return dockerOfficial("postgres", "latest");
  }

  public static DockerImageName mariadb() {
    return dockerOfficial("mariadb", "10");
  }

  public static DockerImageName valkey() {
    return DockerImageName.parse(
        pick("valkey/valkey:8.1", "public.ecr.aws/valkey/valkey:8.1"));
  }

  /** Used as a Dockerfile {@code FROM}, so this returns the raw coordinate. */
  public static String xrayDaemon() {
    return pick("amazon/aws-xray-daemon", "public.ecr.aws/xray/aws-xray-daemon");
  }

  public static DockerImageName otelCollector() {
    return DockerImageName.parse(pick(
        "amazon/aws-otel-collector",
        "public.ecr.aws/aws-observability/aws-otel-collector"));
  }

  /**
   * Note: this image version may need to be occasionally updated to keep it up-to-date and prevent
   * toxiproxy issues.
   */
  public static DockerImageName toxiproxy() {
    return DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.11.0");
  }

  /** Used as a Dockerfile {@code FROM}, so this returns the raw coordinate. */
  public static String postgis() {
    return "postgis/postgis:16-3.4";
  }

  /**
   * Resolves a Docker Official Image and marks it as a substitute for its canonical Docker Hub name,
   * which the Testcontainers database container classes assert against.
   */
  private static DockerImageName dockerOfficial(String repository, String tag) {
    return DockerImageName.parse(dockerOfficialImage(repository, tag))
        .asCompatibleSubstituteFor(repository);
  }

  private static String dockerOfficialImage(String repository, String tag) {
    return (USE_ECR ? ECR_DOCKER_LIBRARY : "") + repository + ":" + tag;
  }

  private static String pick(String dockerHubImage, String ecrImage) {
    return USE_ECR ? ecrImage : dockerHubImage;
  }

  private static boolean resolveUseEcr() {
    String registry = System.getProperty("test-container-registry");
    if (StringUtils.isNullOrEmpty(registry)) {
      registry = System.getenv("TEST_CONTAINER_REGISTRY");
    }
    if (StringUtils.isNullOrEmpty(registry)) {
      registry = DOCKER_HUB;
    }
    final String resolved = registry.trim().toLowerCase(Locale.ROOT);

    if (!ECR.equals(resolved) && !DOCKER_HUB.equals(resolved)) {
      throw new IllegalArgumentException(String.format(
          "Unsupported container registry '%s'. Expected '%s' or '%s'.", resolved, DOCKER_HUB, ECR));
    }

    LOGGER.finest(() -> "Pulling integration test container images from: " + resolved);
    return ECR.equals(resolved);
  }
}
