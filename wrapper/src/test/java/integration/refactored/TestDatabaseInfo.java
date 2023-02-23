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

package integration.refactored;

import java.util.ArrayList;
import java.util.List;

public class TestDatabaseInfo {

  private String username;
  private String password;
  private String defaultDbName;

  private String clusterEndpoint; // "ABC.cluster-XYZ.us-west-2.rds.amazonaws.com"
  private int clusterEndpointPort;

  private String clusterReadOnlyEndpoint; // "ABC.cluster-ro-XYZ.us-west-2.rds.amazonaws.com"
  private int clusterReadOnlyEndpointPort;

  private String instanceEndpointSuffix; // "XYZ.us-west-2.rds.amazonaws.com"
  private int instanceEndpointPort;

  private final ArrayList<TestInstanceInfo> instances = new ArrayList<>();

  public TestDatabaseInfo() {}

  public List<TestInstanceInfo> getInstances() {
    return this.instances;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setDefaultDbName(String defaultDbName) {
    this.defaultDbName = defaultDbName;
  }

  public String getUsername() {
    return this.username;
  }

  public String getPassword() {
    return this.password;
  }

  public String getDefaultDbName() {
    return this.defaultDbName;
  }

  public String getClusterEndpoint() {
    return this.clusterEndpoint;
  }

  public int getClusterEndpointPort() {
    return this.clusterEndpointPort;
  }

  public String getClusterReadOnlyEndpoint() {
    return this.clusterReadOnlyEndpoint;
  }

  public int getClusterReadOnlyEndpointPort() {
    return this.clusterReadOnlyEndpointPort;
  }

  public void setClusterEndpoint(String clusterEndpoint, int clusterEndpointPort) {
    this.clusterEndpoint = clusterEndpoint;
    this.clusterEndpointPort = clusterEndpointPort;
  }

  public void setClusterReadOnlyEndpoint(
      String clusterReadOnlyEndpoint, int clusterReadOnlyEndpointPort) {
    this.clusterReadOnlyEndpoint = clusterReadOnlyEndpoint;
    this.clusterReadOnlyEndpointPort = clusterReadOnlyEndpointPort;
  }

  public void setInstanceEndpointSuffix(
      String instanceEndpointSuffix, int instanceEndpointPort) {
    this.instanceEndpointSuffix = instanceEndpointSuffix;
    this.instanceEndpointPort = instanceEndpointPort;
  }

  public String getInstanceEndpointSuffix() {
    return this.instanceEndpointSuffix;
  }

  public int getInstanceEndpointPort() {
    return this.instanceEndpointPort;
  }

  public TestInstanceInfo getInstance(String instanceName) {
    for (TestInstanceInfo instance : this.instances) {
      if (instanceName != null && instanceName.equals(instance.getInstanceId())) {
        return instance;
      }
    }
    throw new RuntimeException("Instance " + instanceName + " not found.");
  }

  public void moveInstanceFirst(String instanceName) {
    for (int i = 0; i < this.instances.size(); i++) {
      TestInstanceInfo currentInstance = this.instances.get(i);
      if (instanceName != null && instanceName.equals(currentInstance.getInstanceId())) {
        // move this instance to position 0
        this.instances.remove(i);
        this.instances.add(0, currentInstance);
        return;
      }
    }
    throw new RuntimeException("Instance " + instanceName + " not found.");
  }
}
