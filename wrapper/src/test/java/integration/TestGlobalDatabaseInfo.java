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

package integration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * What the tests know about an Aurora global database beyond the one cluster they connect to.
 *
 * <p>{@link TestDatabaseInfo} describes a single cluster and cannot describe this: a global database has a
 * cluster per region, each with its own endpoints and - the part that matters most - its own instance endpoint
 * suffix. The GDB plugins need one instance host template per region, and the suffixes differ between regions of
 * the same database, so they cannot be derived from the region a test happens to be connected to.
 *
 * <p>{@link #getPrimaryRegion()} is where the writer was when the environment was built. A planned switchover
 * moves it, so a test that performs one must not read this afterwards and expect the truth; it is the starting
 * point, which is what a test needs in order to decide what to assert.
 */
public class TestGlobalDatabaseInfo {

  private String globalClusterIdentifier;
  private String primaryRegion;
  private String globalEndpoint;
  private List<TestRegionalClusterInfo> regions = new ArrayList<>();

  public String getGlobalClusterIdentifier() {
    return this.globalClusterIdentifier;
  }

  public void setGlobalClusterIdentifier(final String globalClusterIdentifier) {
    this.globalClusterIdentifier = globalClusterIdentifier;
  }

  public String getPrimaryRegion() {
    return this.primaryRegion;
  }

  public void setPrimaryRegion(final String primaryRegion) {
    this.primaryRegion = primaryRegion;
  }

  /**
   * Returns the endpoint that resolves to whichever region currently holds the writer.
   *
   * <p>A different kind of address from anything in {@link #getRegions()}, not a convenience alias for one. A
   * regional writer endpoint names a cluster and answers in every region; this names the global database, carries
   * no region in its DNS name, and follows the writer across a switchover. The driver classifies it as its own
   * URL type and behaves differently for it, so a test that substituted a regional endpoint here would exercise
   * the ordinary path and report success.
   *
   * @return the global endpoint hostname, or {@code null} when this global cluster published none
   */
  public String getGlobalEndpoint() {
    return this.globalEndpoint;
  }

  public void setGlobalEndpoint(final String globalEndpoint) {
    this.globalEndpoint = globalEndpoint;
  }

  /**
   * Returns every region's cluster, the region that was primary first.
   *
   * @return the regional clusters
   */
  public List<TestRegionalClusterInfo> getRegions() {
    return this.regions;
  }

  public void setRegions(final List<TestRegionalClusterInfo> regions) {
    this.regions = regions == null ? new ArrayList<>() : regions;
  }

  /**
   * Returns the cluster in a named region.
   *
   * @param region the AWS region
   * @return the cluster
   * @throws IllegalArgumentException if this database has no cluster there, which is a test asking for a region
   *     the environment was not built with rather than a condition to handle
   */
  public TestRegionalClusterInfo getRegion(final String region) {
    for (final TestRegionalClusterInfo cluster : this.regions) {
      if (cluster.getRegion().equals(region)) {
        return cluster;
      }
    }
    throw new IllegalArgumentException(
        "This global database has no cluster in " + region + ". It has " + regionNames() + ".");
  }

  /**
   * Returns the clusters in regions other than the one that was primary at provisioning time.
   *
   * @return the secondary clusters, never empty in a valid global database environment
   */
  public List<TestRegionalClusterInfo> getSecondaryRegions() {
    final List<TestRegionalClusterInfo> secondaries = new ArrayList<>();
    for (final TestRegionalClusterInfo cluster : this.regions) {
      if (!cluster.getRegion().equals(this.primaryRegion)) {
        secondaries.add(cluster);
      }
    }
    return Collections.unmodifiableList(secondaries);
  }

  /**
   * Returns the first secondary region's cluster.
   *
   * <p>What a test means by "a region that is not primary" when it does not care which. Most GDB tests run
   * against a secondary and any of them will do.
   *
   * @return the first secondary cluster
   * @throws IllegalStateException if the environment has no secondary region, which a global database
   *     environment cannot legitimately be in
   */
  public TestRegionalClusterInfo getFirstSecondaryRegion() {
    final List<TestRegionalClusterInfo> secondaries = getSecondaryRegions();
    if (secondaries.isEmpty()) {
      throw new IllegalStateException(
          "This global database reports only " + regionNames() + ", so it has no secondary region. A global "
              + "database environment is provisioned with at least one.");
    }
    return secondaries.get(0);
  }

  /**
   * Returns the instance host templates the GDB plugins need, one per region.
   *
   * <p>The {@code globalClusterInstanceHostPatterns} value: {@code ?.<suffix>} for each region, comma
   * separated. Built here rather than in each test because getting it wrong produces unresolvable hostnames and
   * a failure that looks like broken topology discovery rather than a missing parameter.
   *
   * @return the parameter value
   */
  public String getInstanceHostPatterns() {
    final StringBuilder patterns = new StringBuilder();
    for (final TestRegionalClusterInfo cluster : this.regions) {
      if (cluster.getInstanceEndpointSuffix() == null) {
        continue;
      }
      if (patterns.length() > 0) {
        patterns.append(',');
      }
      patterns.append("?.").append(cluster.getInstanceEndpointSuffix());
    }
    return patterns.toString();
  }

  /**
   * Returns every region this database spans.
   *
   * @return the region names, the primary first
   */
  public List<String> getRegionNames() {
    final List<String> names = new ArrayList<>();
    for (final TestRegionalClusterInfo cluster : this.regions) {
      names.add(cluster.getRegion());
    }
    return Collections.unmodifiableList(names);
  }

  private String regionNames() {
    return getRegionNames().toString();
  }
}
