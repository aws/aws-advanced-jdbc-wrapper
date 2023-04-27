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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashSet;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TestEnvironmentRequest {

  @JsonIgnore
  private int envPreCreateIndex;

  @JsonProperty("engine")
  private DatabaseEngine engine;

  @JsonProperty("instances")
  private DatabaseInstances instances;

  @JsonProperty("deployment")
  private DatabaseEngineDeployment deployment;

  @JsonProperty("targetJvm")
  private TargetJvm targetJvm;

  @JsonProperty("features")
  private final Set<TestEnvironmentFeatures> features = new HashSet<>();

  @JsonProperty("numOfInstances")
  private int numOfInstances = 1;

  // This constructor should NOT be used in the code. It's required for serialization.
  public TestEnvironmentRequest() {}

  public TestEnvironmentRequest(
      DatabaseEngine engine,
      DatabaseInstances instances,
      int numOfInstances,
      DatabaseEngineDeployment deployment,
      TargetJvm targetJvm,
      TestEnvironmentFeatures... features) {

    this.engine = engine;
    this.instances = instances;
    this.deployment = deployment;
    this.targetJvm = targetJvm;
    this.numOfInstances = numOfInstances;

    if (features != null) {
      for (TestEnvironmentFeatures feature : features) {
        if (feature != null) {
          this.features.add(feature);
        }
      }
    }
  }

  @JsonIgnore
  public DatabaseEngine getDatabaseEngine() {
    return this.engine;
  }

  @JsonIgnore
  public DatabaseInstances getDatabaseInstances() {
    return this.instances;
  }

  @JsonIgnore
  public DatabaseEngineDeployment getDatabaseEngineDeployment() {
    return this.deployment;
  }

  @JsonIgnore
  public TargetJvm getTargetJvm() {
    return this.targetJvm;
  }

  @JsonIgnore
  public Set<TestEnvironmentFeatures> getFeatures() {
    return this.features;
  }

  @JsonIgnore
  public int getNumOfInstances() {
    return this.numOfInstances;
  }

  @JsonIgnore
  public String getDisplayName() {
    return String.format(
        "Test environment [%s, %s, %s, %s, %d, %s]",
        deployment, engine, targetJvm, instances, numOfInstances, features);
  }

  @JsonIgnore
  public int getEnvPreCreateIndex() {
    return this.envPreCreateIndex;
  }

  @JsonIgnore
  public void setEnvPreCreateIndex(int index) {
    this.envPreCreateIndex = index;
  }
}
