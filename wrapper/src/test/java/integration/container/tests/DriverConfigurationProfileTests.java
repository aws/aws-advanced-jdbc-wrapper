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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.condition.DisableOnTestFeature;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.ExecutionTimeConnectionPluginFactory;
import software.amazon.jdbc.profile.ConfigurationProfileBuilder;
import software.amazon.jdbc.profile.DriverConfigurationProfiles;
import software.amazon.jdbc.wrapper.ConnectionWrapper;
import software.amazon.jdbc.wrapper.ResultSetWrapper;
import software.amazon.jdbc.wrapper.StatementWrapper;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT})
@Order(7)
public class DriverConfigurationProfileTests {

  @TestTemplate
  public void testOpenConnectionWithUnknownProfile() {

    Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PROFILE_NAME.name, "unknownProfile");

    SQLException actualException =
        assertThrows(
            SQLException.class,
            () -> DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props));

    assertTrue(actualException.getMessage().contains("unknownProfile"));
  }

  @TestTemplate
  public void testOpenConnectionWithProfile() throws SQLException {

    Properties props = ConnectionStringHelper.getDefaultProperties();
    props.setProperty(PropertyDefinition.PROFILE_NAME.name, "testProfile");

    DriverConfigurationProfiles.clear();
    ConfigurationProfileBuilder.get()
        .withName("testProfile")
        .withPluginFactories(Collections.singletonList(ExecutionTimeConnectionPluginFactory.class))
        .buildAndSet();

    Connection conn = DriverManager.getConnection(ConnectionStringHelper.getWrapperUrl(), props);

    assertTrue(conn instanceof ConnectionWrapper);
    assertTrue(conn.isWrapperFor(DriverHelper.getConnectionClass()));

    assertTrue(conn.isValid(10));

    Statement statement = conn.createStatement();
    assertNotNull(statement);
    assertTrue(statement instanceof StatementWrapper);

    int rnd = new Random().nextInt(100);
    ResultSet resultSet = statement.executeQuery("SELECT " + rnd);
    assertNotNull(resultSet);
    assertTrue(resultSet instanceof ResultSetWrapper);

    resultSet.next();
    int result = resultSet.getInt(1);
    assertEquals(rnd, result);

    conn.close();
  }
}
