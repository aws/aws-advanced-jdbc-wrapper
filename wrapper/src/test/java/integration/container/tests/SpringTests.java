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

import static org.junit.jupiter.api.Assertions.assertEquals;

import integration.DatabaseEngineDeployment;
import integration.TestEnvironmentFeatures;
import integration.TestEnvironmentInfo;
import integration.TestEnvironmentRequest;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import java.util.Properties;
import java.util.Random;
import javax.sql.DataSource;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import software.amazon.jdbc.PropertyDefinition;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT})
@Order(13)
public class SpringTests {

  @TestTemplate
  public void testOpenConnection() {

    JdbcTemplate jdbcTemplate = new JdbcTemplate(getDataSource());

    int rnd = new Random().nextInt(100);
    Integer result = jdbcTemplate.queryForObject("SELECT " + rnd, Integer.class);
    assertEquals(rnd, result);
  }

  private DataSource getDataSource() {
    final TestEnvironmentInfo envInfo = TestEnvironment.getCurrent().getInfo();

    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName("software.amazon.jdbc.Driver");
    dataSource.setUrl(ConnectionStringHelper.getWrapperUrl());
    dataSource.setUsername(envInfo.getDatabaseInfo().getUsername());

    Properties props = ConnectionStringHelper.getDefaultPropertiesWithNoPlugins();

    // DSQL only supports IAM authentication.
    final TestEnvironmentRequest request = envInfo.getRequest();
    if (request.getDatabaseEngineDeployment() == DatabaseEngineDeployment.DSQL) {
      props.setProperty(PropertyDefinition.PLUGINS.name, "iamDsql");
    } else {
      dataSource.setPassword(envInfo.getDatabaseInfo().getPassword());
    }

    props.setProperty(PropertyDefinition.LOGGER_LEVEL.name, "ALL");
    dataSource.setConnectionProperties(props);

    return dataSource;
  }
}
