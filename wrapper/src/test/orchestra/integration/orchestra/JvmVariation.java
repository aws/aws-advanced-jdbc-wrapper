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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import software.amazon.orchestra.Composition;
import software.amazon.orchestra.Variation;
import software.amazon.orchestra.instruments.docker.JavaTestContainerConfiguration;

/**
 * The target-JVM axis: one composition per JDK the suite runs on.
 *
 * <p>Replaces the {@code TargetJvm} loop in {@code TestEnvironmentProvider}, which iterated every value of
 * the enum and switched slots off with five {@code test-no-openjdk*} flags plus {@code test-no-graalvm}.
 *
 * <p>A pure {@code withOverride} of the container image, with no instrument rebinding, which is only possible
 * because {@link TestShapeInstrumentDefinition} now derives the JVM it publishes from that same image. Both
 * read {@link JavaTestContainerConfiguration#getTestContainerImage()} through {@link TargetJvmImages}, so one
 * override moves the container and the published shape together. The alternative - passing the JVM to the
 * shape as well - is what the engine axis has to do for the engine, and it is the reason this axis could not
 * simply be added beside it: variations chain, so a second axis rebuilding the shape would have to know what
 * the engine axis had already chosen, and would silently undo it if it guessed wrong.
 *
 * <p>Images are not validated against what Docker can pull. A JVM the table knows but the host cannot fetch
 * is the container instrument's error to report, with the real Docker message, rather than something to
 * second-guess here.
 */
public class JvmVariation implements Variation {

  private final TargetJvm[] jvms;

  /**
   * Returns the JVMs a run asked for, from {@code -Dorchestra-jvms}.
   *
   * <p>Here rather than in each runner because both of them need it and they must agree: the property is the
   * axis's interface, and two copies of the parsing would be two places for the default to drift.
   *
   * <p>The harness expresses this axis by exclusion - {@code test-no-openjdk8} through
   * {@code test-no-openjdk24}, plus {@code test-no-graalvm} - and this by naming what to include, which is
   * the same choice the engine and driver axes made.
   *
   * <p>Defaults to Java 21 alone, which is what both runners did before the axis existed. A default of every
   * JVM would multiply an hour-long AWS run by six, and the JVM is the axis least likely to be the one a
   * developer is investigating.
   *
   * @return at least one JVM
   */
  static TargetJvm[] requested() {
    final String property = System.getProperty("orchestra-jvms");
    if (property == null || property.trim().isEmpty()) {
      return new TargetJvm[] {TargetJvm.OPENJDK21};
    }

    final String[] names = property.split(",");
    final TargetJvm[] jvms = new TargetJvm[names.length];
    for (int i = 0; i < names.length; i++) {
      jvms[i] = TargetJvm.valueOf(names[i].trim().toUpperCase(Locale.ROOT));
    }
    return jvms;
  }

  /**
   * Creates the axis.
   *
   * @param jvms the JVMs to run on, in order; at least one
   */
  public JvmVariation(final TargetJvm... jvms) {
    if (jvms == null || jvms.length == 0) {
      throw new IllegalArgumentException(
          "A JvmVariation needs at least one JVM, otherwise it would produce no compositions at all.");
    }
    this.jvms = jvms.clone();
  }

  @Override
  public List<Composition> process(final List<Composition> compositions) {
    final List<Composition> expanded = new ArrayList<>(compositions.size() * this.jvms.length);

    for (final Composition composition : compositions) {
      for (final TargetJvm jvm : this.jvms) {
        expanded.add(composition
            .withOverride(
                JavaTestContainerConfiguration.class,
                "getTestContainerImage",
                TargetJvmImages.imageFor(jvm))
            // Appended rather than replacing, so a name records every axis that produced the slot and a
            // failure can be attributed to the whole combination.
            .withDisplayName(append(composition.getDisplayName(), label(jvm))));
      }
    }
    return expanded;
  }

  private static String label(final TargetJvm jvm) {
    return jvm.name().toLowerCase(Locale.ROOT);
  }

  private static String append(final String current, final String label) {
    return current == null || current.isEmpty() || "default".equals(current)
        ? label
        : current + "-" + label;
  }
}
