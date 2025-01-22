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

package integration.host;

import integration.TestEnvironmentRequest;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestEnvironmentProvider.class)
public class TestRunner {

  @TestTemplate
  public void runTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {

    try (final TestEnvironment env = TestEnvironment.build(testEnvironmentRequest)) {
      env.runTests("in-container");
    }
  }

  @TestTemplate
  public void debugTests(TestEnvironmentRequest testEnvironmentRequest) throws Exception {

    try (final TestEnvironment env = TestEnvironment.build(testEnvironmentRequest)) {
      env.debugTests("in-container");
    }
  }
}
