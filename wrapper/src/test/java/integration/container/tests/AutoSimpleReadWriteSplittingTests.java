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

package integration.container.tests;

import integration.TestEnvironmentFeatures;
import integration.container.TestDriverProvider;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.EnableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
import java.sql.SQLException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for the SQL-driven, endpoint-based {@code autoSimpleReadWriteSplitting} plugin.
 * It reuses the {@link SimpleReadWriteSplittingTests} suite (configured write/read endpoints) but
 * routes queries based on SQL analysis. As with {@link AutoReadWriteSplittingTests}, base tests that
 * execute un-hinted raw read SQL and expect it to stay on the current connection are disabled,
 * because SQL-based routing intentionally reroutes such reads.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature(TestEnvironmentFeatures.FAILOVER_SUPPORTED)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_ENCRYPTION_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@EnableOnNumOfInstances(min = 2)
@MakeSureFirstInstanceWriter
@Order(22)
public class AutoSimpleReadWriteSplittingTests extends SimpleReadWriteSplittingTests {

  {
    // Route via the SQL parser; the sqlParser plugin must precede the read/write splitting plugin.
    pluginCode = "sqlParser,autoSimpleReadWriteSplitting";
    pluginCodesWithFailover = "failover2,efm2,sqlParser,autoSimpleReadWriteSplitting";
  }

  @TestTemplate
  @Disabled("SQL routing reroutes 'START TRANSACTION READ ONLY' before the transaction is active, "
      + "so this setReadOnly-oriented expectation does not apply to autoSimpleReadWriteSplitting.")
  @Override
  public void test_setReadOnlyFalseInReadOnlyTransaction() throws SQLException {
  }

  @TestTemplate
  @Disabled("SQL routing may reroute the post-write SELECT to the read endpoint (subject to "
      + "replication lag), so this expectation does not apply to autoSimpleReadWriteSplitting.")
  @Override
  public void test_setReadOnlyTrueInTransaction() throws SQLException {
  }

  @TestTemplate
  @Disabled("SQL routing reroutes the raw SELECT to the read endpoint, changing the stale-statement "
      + "semantics this test relies on; not applicable to autoSimpleReadWriteSplitting.")
  @Override
  public void test_executeWithOldConnection() throws SQLException {
  }

  @TestTemplate
  @Disabled("With autocommit disabled the connection is pinned to preserve the transaction, so "
      + "setReadOnly(false) does not switch back to the writer here; this setReadOnly-oriented "
      + "expectation does not apply to autoSimpleReadWriteSplitting.")
  @Override
  public void test_setReadOnlyFalseInTransaction_setAutocommitFalse() throws SQLException {
  }

  @TestTemplate
  @Disabled("With autocommit disabled the connection is pinned, so setReadOnly(true) does not "
      + "switch to the read endpoint; this expectation does not apply to autoSimpleReadWriteSplitting.")
  @Override
  public void test_autoCommitStatePreserved_acrossConnectionSwitches() throws SQLException {
  }
}
