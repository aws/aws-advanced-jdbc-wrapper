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

import integration.TargetJvm;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The one place a {@code TargetJvm} and a container image are paired.
 *
 * <p>Both directions, from one table, and that is the whole point. The JVM axis works by overriding the
 * container image, while the in-container suite gates on the {@code TargetJvm} the shape publishes - so if
 * the two were derived separately, a slot could run on Java 8 while every {@code @EnableOnTargetJvm}
 * condition believed it was Java 21. Reading both from here makes that disagreement unrepresentable, which
 * is the same reason the instance count moved into configuration.
 *
 * <p>The images are the harness's, copied from {@code integration.host.TestEnvironment}'s
 * {@code getContainerBaseImageName}. Copied rather than referenced because that method is private to the
 * legacy harness and goes away with it; the pairing has to outlive it.
 *
 * <p>{@code GRAALVM} is present for completeness and is the one entry the harness itself pins to an old
 * release ({@code 22.2.0}). Nothing on this path has run against it.
 */
final class TargetJvmImages {

  /**
   * The harness's image per JVM.
   *
   * <p>Ordered so that a matrix listing several JVMs produces slots in a predictable order, which matters
   * only because the display names end up in artifact directory names.
   */
  private static final Map<TargetJvm, String> IMAGES;

  static {
    final Map<TargetJvm, String> images = new LinkedHashMap<>();
    images.put(TargetJvm.OPENJDK8, "amazoncorretto:8-alpine");
    images.put(TargetJvm.OPENJDK11, "amazoncorretto:11.0.19-alpine3.17");
    images.put(TargetJvm.OPENJDK17, "amazoncorretto:17-alpine3.21");
    images.put(TargetJvm.OPENJDK21, "amazoncorretto:21-alpine-full");
    images.put(TargetJvm.OPENJDK24, "amazoncorretto:24-alpine-full");
    images.put(TargetJvm.GRAALVM, "ghcr.io/graalvm/jdk:22.2.0");
    IMAGES = Collections.unmodifiableMap(images);
  }

  private TargetJvmImages() {
  }

  /**
   * Returns the image that provides a JVM.
   *
   * @param jvm the JVM to run on
   * @return the image reference
   * @throws IllegalArgumentException if no image is paired with it, rather than guessing one
   */
  static String imageFor(final TargetJvm jvm) {
    final String image = IMAGES.get(jvm);
    if (image == null) {
      throw new IllegalArgumentException(
          "No container image is paired with " + jvm + ". Add the pairing to TargetJvmImages rather than "
              + "letting a run report a JVM it is not on.");
    }
    return image;
  }

  /**
   * Returns the JVM an image provides.
   *
   * <p>The reverse lookup the shape uses, and it is exact rather than fuzzy: {@code amazoncorretto:21-alpine}
   * and {@code amazoncorretto:21-alpine-full} are different images, and a substring match that treated them
   * as one would be right until the day the tags diverged in something that matters.
   *
   * <p>An unknown image is an error rather than a default, because the alternative is publishing a JVM name
   * that is a guess. The in-container conditions act on that name, so a wrong one does not fail - it silently
   * runs or skips the wrong tests.
   *
   * @param image the configured container image
   * @return the JVM it provides
   * @throws IllegalArgumentException if the image is not in the table
   */
  static TargetJvm jvmFor(final String image) {
    for (final Map.Entry<TargetJvm, String> entry : IMAGES.entrySet()) {
      if (entry.getValue().equals(image)) {
        return entry.getKey();
      }
    }
    throw new IllegalArgumentException(
        "The container image '" + image + "' is not paired with any TargetJvm, so this run cannot say which "
            + "JVM it is on - and the in-container conditions gate on exactly that. Add the pairing to "
            + "TargetJvmImages, or configure one of: " + IMAGES.values() + ".");
  }
}
