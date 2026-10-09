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

import integration.TestTags;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import software.amazon.orchestra.instruments.aws.AuroraGlobalClusterConfiguration;

/**
 * Configuration for a run against an Aurora global database.
 *
 * <p>Everything an Aurora run needs, plus the regions. It extends {@link OrchestraAuroraConfig} rather than
 * replacing it because a global database is made of Aurora clusters: the credentials, database name, instance
 * class, container copies and in-container properties all mean the same thing, and duplicating them would give
 * a global run its own quietly diverging copy.
 *
 * <p>A subclass rather than adding these methods to the Aurora configuration, so a single-region run cannot
 * accidentally look like a global one. The multi-region whitelist and the orphan sweep both ask a configuration
 * whether it describes a global database; answering "yes, with no secondaries" on every ordinary run would make
 * them do work with no reason to.
 */
public class OrchestraGlobalDatabaseConfig extends OrchestraAuroraConfig
    implements AuroraGlobalClusterConfiguration {

  /** Where the secondary regions come from. */
  static final String SECONDARY_REGIONS_PROPERTY = "orchestra-secondary-regions";

  /** How many instances each secondary region gets. */
  static final String SECONDARY_INSTANCES_PROPERTY = "orchestra-secondary-instances";

  /**
   * Creates the configuration for a global database run.
   *
   * @param gradleTask the task to run inside the container, normally {@code in-container}
   */
  public OrchestraGlobalDatabaseConfig(final String gradleTask) {
    super(gradleTask);
  }

  /**
   * Returns the secondary regions, from {@code -Dorchestra-secondary-regions}.
   *
   * <p>Defaults to one region rather than none, because none is not a global database and the instrument
   * rejects it. One is also what a run should default to: each additional region is another pair of instances
   * and another twenty to forty minutes of provisioning, so "more regions" is a decision worth making
   * explicitly rather than inheriting.
   *
   * <p>{@code us-west-2} pairs with the {@code us-east-2} default primary: a different continent-scale distance
   * is what makes cross-region latency and replication lag visible rather than noise.
   *
   * @return at least one secondary region
   */
  @Override
  public List<String> getGlobalSecondaryRegions() {
    final String requested = System.getProperty(SECONDARY_REGIONS_PROPERTY);
    if (requested == null || requested.trim().isEmpty()) {
      return List.of("us-west-2");
    }

    final List<String> regions = new ArrayList<>();
    for (final String region : requested.split(",")) {
      if (!region.trim().isEmpty()) {
        regions.add(region.trim());
      }
    }
    return regions;
  }

  /**
   * Returns how many instances each secondary region gets, from {@code -Dorchestra-secondary-instances}.
   *
   * <p>Two by default, and two is the minimum that tests anything: a secondary region holds only readers, so
   * failing over inside one needs a second reader to land on, and read/write splitting needs a choice of
   * readers to make.
   *
   * @return the instance count per secondary region
   */
  @Override
  public int getGlobalSecondaryInstanceCount() {
    final String requested = System.getProperty(SECONDARY_INSTANCES_PROPERTY);
    if (requested == null || requested.trim().isEmpty()) {
      return 2;
    }
    return Integer.parseInt(requested.trim());
  }

  /**
   * Returns the in-container properties, with the GDB tests selected.
   *
   * <p>Tag-based, and the reason is that the GDB classes and the ordinary ones exclude each other by tag rather
   * than by feature. A run that provisioned a global database and then executed the whole ordinary suite would
   * spend an hour re-testing single-region behaviour against a topology built for something else, and the
   * classes that assume a local writer would fail for reasons that say nothing about global databases.
   *
   * <p>An explicit {@code -Dtest-include-tags} still wins, because {@link OrchestraAuroraConfig} applies the
   * command line after the suite - which is how a developer runs one GDB class at a time.
   */
  @Override
  public Map<String, String> getTestContainerSystemProperties() {
    final Map<String, String> properties = new LinkedHashMap<>(super.getTestContainerSystemProperties());

    // putIfAbsent, not put: the parent has already applied an explicitly passed -Dtest-include-tags, and that
    // has to win. Running one GDB class at a time is how these get developed, and a run that silently replaced
    // the selection would make that impossible.
    properties.putIfAbsent("test-include-tags", TestTags.GDB);

    // And the exclusion has to go, or including the tag achieves nothing. The standard suite excludes gdb to
    // keep these classes out of ordinary runs, and JUnit resolves a tag that is both included and excluded as
    // excluded - so the first real run provisioned a global database in two regions, waited fifty minutes, and
    // then failed with "No tests found". Gradle warned about the contradiction; nothing acted on it.
    dropGdbExclusion(properties);
    return properties;
  }

  /**
   * Removes the {@code gdb} tag from the exclusions, keeping any others.
   *
   * <p>Rewritten rather than cleared, because a suite may exclude tags for its own unrelated reasons and this
   * knows only about one of them.
   */
  private static void dropGdbExclusion(final Map<String, String> properties) {
    final String excluded = properties.get("test-exclude-tags");
    if (excluded == null || excluded.trim().isEmpty()) {
      return;
    }

    final List<String> remaining = new ArrayList<>();
    for (final String tag : excluded.split(",")) {
      final String trimmed = tag.trim();
      if (!trimmed.isEmpty() && !TestTags.GDB.equals(trimmed)) {
        remaining.add(trimmed);
      }
    }

    if (remaining.isEmpty()) {
      properties.remove("test-exclude-tags");
    } else {
      properties.put("test-exclude-tags", String.join(",", remaining));
    }
  }
}
