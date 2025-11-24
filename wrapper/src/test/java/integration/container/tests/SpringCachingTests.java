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
import static software.amazon.jdbc.plugin.cache.CacheConnection.CACHE_RW_ENDPOINT_ADDR;

import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import javax.sql.DataSource;
import org.junit.jupiter.api.*;
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
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@Order(13)
public class SpringCachingTests {

  @TestTemplate
  public void testOpenConnection() {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(getDataSource());

    int rnd = new Random().nextInt(100);
    Integer result = jdbcTemplate.queryForObject("/* CACHE_PARAM(ttl=100s) */ SELECT " + rnd, Integer.class);
    assertEquals(rnd, result);

    String SQL_FIND = "/*+ CACHE_PARAM(ttl=60s) */ SELECT ?";

    List<Object> res = jdbcTemplate.query(
        con -> {
          PreparedStatement ps = con.prepareStatement(SQL_FIND);
          ps.setString(1, Integer.toString(rnd));   // user input as parameter
          return ps;
        },
        (rs, rowNum) -> rs.getInt(1));

    assertEquals(1, res.size());
  }

  private DataSource getDataSource() {
    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName("software.amazon.jdbc.Driver");
    dataSource.setUrl(ConnectionStringHelper.getWrapperUrl());
    System.out.println("Datasource URL is " + ConnectionStringHelper.getWrapperUrl());
    dataSource.setUsername(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    dataSource.setPassword(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());

    Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.LOGGER_LEVEL.name, "ALL");
    props.setProperty(PropertyDefinition.PLUGINS.name, "dataRemoteCache");
    props.setProperty(CACHE_RW_ENDPOINT_ADDR.name, "dev-dsk-quchen-2a-3a165932.us-west-2.amazon.com:6379");

    dataSource.setConnectionProperties(props);

    return dataSource;
  }
}
