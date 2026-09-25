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
import java.util.List;

/**
 * One region's cluster within an Aurora global database.
 *
 * <p>Separate from {@link TestDatabaseInfo} rather than reusing it, because the two are used differently and
 * conflating them would invite a mistake. {@code TestDatabaseInfo} is what the suite <em>connects through</em>,
 * including proxied endpoints and a flat instance list whose first entry the tests treat as the writer. This is
 * a description of a region, and in a secondary region there is no writer to be first.
 */
public class TestRegionalClusterInfo {

  private String region;
  private String clusterIdentifier;
  private String clusterEndpoint;
  private String clusterReadOnlyEndpoint;
  private int port;
  private String instanceEndpointSuffix;
  private List<String> instanceIdentifiers = new ArrayList<>();

  public String getRegion() {
    return this.region;
  }

  public void setRegion(final String region) {
    this.region = region;
  }

  public String getClusterIdentifier() {
    return this.clusterIdentifier;
  }

  public void setClusterIdentifier(final String clusterIdentifier) {
    this.clusterIdentifier = clusterIdentifier;
  }

  /**
   * Returns this region's writer cluster endpoint.
   *
   * <p>Connectable in every region and writable only in the primary one. A secondary region's writer endpoint
   * answers - that is what makes a switchover invisible to a connection string - so a test that wants a reader
   * must ask for the reader endpoint rather than expecting this one to refuse.
   *
   * @return the writer cluster endpoint
   */
  public String getClusterEndpoint() {
    return this.clusterEndpoint;
  }

  public void setClusterEndpoint(final String clusterEndpoint) {
    this.clusterEndpoint = clusterEndpoint;
  }

  public String getClusterReadOnlyEndpoint() {
    return this.clusterReadOnlyEndpoint;
  }

  public void setClusterReadOnlyEndpoint(final String clusterReadOnlyEndpoint) {
    this.clusterReadOnlyEndpoint = clusterReadOnlyEndpoint;
  }

  public int getPort() {
    return this.port;
  }

  public void setPort(final int port) {
    this.port = port;
  }

  /**
   * Returns what follows the instance identifier in this region's instance endpoints.
   *
   * <p>For example {@code XYZ1.us-east-2.rds.amazonaws.com}. Different for every region of the same global
   * database, which is why the GDB plugins take a list of templates rather than one.
   *
   * @return the suffix
   */
  public String getInstanceEndpointSuffix() {
    return this.instanceEndpointSuffix;
  }

  public void setInstanceEndpointSuffix(final String instanceEndpointSuffix) {
    this.instanceEndpointSuffix = instanceEndpointSuffix;
  }

  public List<String> getInstanceIdentifiers() {
    return this.instanceIdentifiers;
  }

  public void setInstanceIdentifiers(final List<String> instanceIdentifiers) {
    this.instanceIdentifiers = instanceIdentifiers == null ? new ArrayList<>() : instanceIdentifiers;
  }

  /**
   * Returns an instance's endpoint in this region.
   *
   * @param instanceIdentifier the instance
   * @return the host name
   */
  public String getInstanceEndpoint(final String instanceIdentifier) {
    return instanceIdentifier + "." + this.instanceEndpointSuffix;
  }

  @Override
  public String toString() {
    return "TestRegionalClusterInfo[" + this.region + " " + this.clusterIdentifier
        + " writer=" + this.clusterEndpoint + " reader=" + this.clusterReadOnlyEndpoint + ":" + this.port
        + ", instances=" + this.instanceIdentifiers + "]";
  }
}
