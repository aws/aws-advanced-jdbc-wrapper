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

package integration.container.aurora.postgres;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

// Tests will run in order of top to bottom.
// To add additional tests, append it inside SelectClasses, comma-separated
@Suite
@SelectClasses({
  AuroraPostgresIntegrationTest.class,
  AuroraPostgresFailoverTest.class,
  AuroraPostgresDataSourceTest.class,
  AuroraPostgresStaleDnsTest.class,
  HikariCPIntegrationTest.class,
  AuroraPostgresReadWriteSplittingTest.class,
  HikariCPReadWriteSplittingTest.class
})
public class AuroraPostgresTestSuite {}
