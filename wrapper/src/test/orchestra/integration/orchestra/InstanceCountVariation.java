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

import java.util.ArrayList;
import java.util.List;
import software.amazon.orchestra.Composition;
import software.amazon.orchestra.Variation;
import software.amazon.orchestra.instruments.aws.AuroraClusterConfiguration;

/**
 * The instance-count axis: one composition per cluster size.
 *
 * <p>Replaces one of {@code TestEnvironmentProvider}'s nested loops, which iterated
 * {@code [1, 2, 3, 5]} and then relied on {@code @EnableOnNumOfInstances} to decide what could run in
 * each. The annotations stay - they are how a test says what it needs - but the loop becomes this.
 *
 * <p>A pure {@code withOverride}, with no instrument rebinding, and that is the point of having made
 * {@link TestShapeInstrumentDefinition} read the count from configuration. Both the cluster and the shape
 * ask {@code AuroraClusterConfiguration.getAuroraInstanceCount()}, so one override moves them together and
 * they cannot disagree about how large the cluster is. While the shape held its own copy this axis could
 * not be expressed safely at all: the cluster would have had N instances and the tests would have gated on
 * whatever number the shape was constructed with.
 *
 * <p>Counts are not validated against what the engine allows. Aurora accepts 1 to 15, and a request outside
 * that is the provisioning instrument's error to report, with the real AWS message, rather than something
 * to second-guess here.
 */
public class InstanceCountVariation implements Variation {

  private final int[] counts;

  /**
   * Creates the axis.
   *
   * @param counts the cluster sizes to run, in order; at least one
   */
  public InstanceCountVariation(final int... counts) {
    if (counts == null || counts.length == 0) {
      throw new IllegalArgumentException(
          "An InstanceCountVariation needs at least one count, otherwise it would produce no "
              + "compositions at all.");
    }
    this.counts = counts.clone();
  }

  @Override
  public List<Composition> process(final List<Composition> compositions) {
    final List<Composition> expanded = new ArrayList<>(compositions.size() * this.counts.length);

    for (final Composition composition : compositions) {
      for (final int count : this.counts) {
        expanded.add(composition
            .withOverride(AuroraClusterConfiguration.class, "getAuroraInstanceCount", count)
            // Appended rather than replacing, so a name records every axis that produced the slot and a
            // failure can be attributed to the whole combination.
            .withDisplayName(append(composition.getDisplayName(), count + "nodes")));
      }
    }
    return expanded;
  }

  private static String append(final String current, final String label) {
    return current == null || current.isEmpty() || "default".equals(current)
        ? label
        : current + "-" + label;
  }
}
