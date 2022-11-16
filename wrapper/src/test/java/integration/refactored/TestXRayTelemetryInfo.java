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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

// Annotation is required to avoid an error when trying to serialize the getUrl method
@JsonIgnoreProperties(ignoreUnknown = true)
public class TestXRayTelemetryInfo {

  private String endpoint;
  private int endpointPort;

  // This constructor should NOT be used in the code. It's required for serialization.
  public TestXRayTelemetryInfo() {
  }

  public TestXRayTelemetryInfo(String endpoint, int endpointPort) {
    this.endpoint = endpoint;
    this.endpointPort = endpointPort;
  }

  public String getEndpoint() {
    return this.endpoint;
  }

  public String getUrl() {
    return getEndpoint() + ":" + getEndpointPort();
  }

  public int getEndpointPort() {
    return this.endpointPort;
  }
}
