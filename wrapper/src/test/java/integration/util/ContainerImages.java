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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Logger;
import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;
import org.testcontainers.utility.DockerImageName;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

/**
 * Single source for Docker Hub images and their Amazon ECR Public mirrors.
 *
 * <p>Docker Hub rate limits anonymous pulls per source IP. Select ECR Public with either:
 *
 * <pre>
 *   ./gradlew orchestra-test-pg-aurora -Dtest-container-registry=ecr
 *   TEST_CONTAINER_REGISTRY=ecr ./gradlew orchestra-test-docker
 * </pre>
 *
 * <p>The system property wins over the environment variable. These values reach Orchestra tasks through
 * each task's explicit property-forwarding list, not through blanket forwarding for all {@code Test} tasks.
 * The default is {@code dockerhub}, which keeps every image where it was before this setting existed.
 *
 * <p>Images that already name a registry, including toxiproxy and GraalVM on {@code ghcr.io}, are unchanged.
 * The {@code postgis/postgis:16-3.4} Docker Hub image is also unchanged because it is used in a Dockerfile
 * {@code FROM} and has no ECR Public mirror. Testcontainers' Ryuk image remains a Docker Hub pull because no
 * ECR Public mirror exists for it.
 */
public final class ContainerImages {

  private static final Logger LOGGER = Logger.getLogger(ContainerImages.class.getName());

  private static final String DOCKER_HUB = "dockerhub";
  private static final String ECR = "ecr";

  /** The ECR Public mirrors, keyed by the unversioned Docker Hub repository. */
  private static final Map<String, String> ECR_MIRRORS;

  static {
    final Map<String, String> mirrors = new LinkedHashMap<>();
    mirrors.put("amazoncorretto", "public.ecr.aws/docker/library/amazoncorretto");
    mirrors.put("mysql", "public.ecr.aws/docker/library/mysql");
    mirrors.put("postgres", "public.ecr.aws/docker/library/postgres");
    mirrors.put("mariadb", "public.ecr.aws/docker/library/mariadb");
    mirrors.put("alpine", "public.ecr.aws/docker/library/alpine");
    mirrors.put("valkey/valkey", "public.ecr.aws/valkey/valkey");
    mirrors.put("amazon/aws-otel-collector", "public.ecr.aws/aws-observability/aws-otel-collector");
    mirrors.put("amazon/aws-xray-daemon", "public.ecr.aws/xray/aws-xray-daemon");
    ECR_MIRRORS = Collections.unmodifiableMap(mirrors);
  }

  private static final boolean USE_ECR = resolveUseEcr();

  private ContainerImages() {
  }

  /** Base image of the container the integration tests themselves run in. */
  public static String jvmBase(final TargetJvm jvm) {
    final String canonical;
    switch (jvm) {
      case OPENJDK8:
        canonical = "amazoncorretto:8-alpine";
        break;
      case OPENJDK11:
        canonical = "amazoncorretto:11.0.19-alpine3.17";
        break;
      case OPENJDK17:
        canonical = "amazoncorretto:17-alpine3.21";
        break;
      case OPENJDK21:
        canonical = "amazoncorretto:21-alpine-full";
        break;
      case OPENJDK24:
        canonical = "amazoncorretto:24-alpine-full";
        break;
      case GRAALVM:
        canonical = "ghcr.io/graalvm/jdk:22.2.0";
        break;
      default:
        throw new NotImplementedException(jvm.toString());
    }
    return format(toConfiguredRegistry(DockerImageName.parse(canonical)));
  }

  /** Returns the configured mirror of an image, or the image itself when no mirror applies. */
  static DockerImageName toRegistry(final DockerImageName requested, final boolean useEcr) {
    if (!useEcr || !StringUtils.isNullOrEmpty(requested.getRegistry())) {
      return requested;
    }

    final String mirror = ECR_MIRRORS.get(requested.getRepository());
    if (mirror == null) {
      return requested;
    }

    return DockerImageName.parse(mirror + ":" + requested.getVersionPart())
        .asCompatibleSubstituteFor(requested);
  }

  /** Applies the registry selected for this test JVM. */
  public static DockerImageName toConfiguredRegistry(final DockerImageName requested) {
    return toRegistry(requested, USE_ECR);
  }

  private static String format(final DockerImageName image) {
    final String registry = image.getRegistry();
    return (StringUtils.isNullOrEmpty(registry) ? "" : registry + "/")
        + image.getRepository() + ":" + image.getVersionPart();
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
      throw new IllegalArgumentException(Messages.get(
          "ContainerImages.unsupportedRegistry", new Object[] {resolved, DOCKER_HUB, ECR}));
    }

    LOGGER.finest(() -> Messages.get("ContainerImages.selectedRegistry", new Object[] {resolved}));
    return ECR.equals(resolved);
  }
}
