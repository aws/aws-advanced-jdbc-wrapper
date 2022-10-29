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

public class TestInstanceInfo {

  private String instanceName; // "instance-1"
  private String endpoint; // "instance-1.ABC.cluster-XYZ.us-west-2.rds.amazonaws.com"
  private int endpointPort;

  // This constructor should NOT be used in the code. It's required for serialization.
  public TestInstanceInfo() {}

  public TestInstanceInfo(String instanceName, String endpoint, int endpointPort) {
    this.instanceName = instanceName;
    this.endpoint = endpoint;
    this.endpointPort = endpointPort;
  }

  public String getInstanceName() {
    return this.instanceName;
  }

  public String getEndpoint() {
    return this.endpoint;
  }

  public int getEndpointPort() {
    return this.endpointPort;
  }
}
