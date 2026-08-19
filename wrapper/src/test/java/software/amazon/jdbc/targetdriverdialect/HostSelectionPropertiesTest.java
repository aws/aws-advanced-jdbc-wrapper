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

package software.amazon.jdbc.targetdriverdialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.util.PropertyUtils;

/**
 * Covers the removal of target driver properties that restrict which node a connection is accepted
 * against. See issue #2096: {@code targetServerType=primary} was inherited by the Blue/Green plugin's
 * monitoring connections, so the monitor watching the green node - a replica, and therefore read-only
 * until promotion - could never connect, and the plugin never observed the switchover finishing.
 */
class HostSelectionPropertiesTest {

  private static final String TARGET_SERVER_TYPE = "targetServerType";

  @Test
  void pgDialectRemovesTargetServerType() {
    final Properties props = new Properties();
    props.setProperty(TARGET_SERVER_TYPE, "primary");
    props.setProperty("user", "someUser");

    final Set<String> removed = new PgTargetDriverDialect().removeHostSelectionProperties(props);

    assertEquals(Collections.singleton(TARGET_SERVER_TYPE), removed);
    assertNull(props.getProperty(TARGET_SERVER_TYPE));
    assertEquals("someUser", props.getProperty("user"),
        "Only node-selection properties may be removed.");
  }

  @Test
  void pgDialectReportsNothingWhenPropertyAbsent() {
    final Properties props = new Properties();
    props.setProperty("user", "someUser");

    final Set<String> removed = new PgTargetDriverDialect().removeHostSelectionProperties(props);

    assertTrue(removed.isEmpty());
    assertEquals(1, props.size());
  }

  @Test
  void otherDialectsRemoveNothingByDefault() {
    for (final TargetDriverDialect dialect : new TargetDriverDialect[] {
        new GenericTargetDriverDialect(),
        new MysqlConnectorJTargetDriverDialect(),
        new MariadbTargetDriverDialect()}) {

      final Properties props = new Properties();
      props.setProperty(TARGET_SERVER_TYPE, "primary");

      assertTrue(dialect.removeHostSelectionProperties(props).isEmpty(),
          dialect.getClass().getSimpleName() + " has no node-selection property to remove.");
      assertEquals("primary", props.getProperty(TARGET_SERVER_TYPE),
          dialect.getClass().getSimpleName() + " must leave unrelated properties alone.");
    }
  }

  @Test
  void propertyUtilsDelegatesToDialect() {
    final Properties props = new Properties();
    props.setProperty(TARGET_SERVER_TYPE, "primary");
    props.setProperty("socketTimeout", "10000");

    PropertyUtils.removeHostSelectionProperties(props, new PgTargetDriverDialect());

    assertNull(props.getProperty(TARGET_SERVER_TYPE));
    assertEquals("10000", props.getProperty("socketTimeout"),
        "Timeouts must survive so monitoring connections behave like application connections.");
  }

  @Test
  void propertyUtilsIsANoOpForDialectsWithoutSuchProperties() {
    final Properties props = new Properties();
    props.setProperty(TARGET_SERVER_TYPE, "primary");

    PropertyUtils.removeHostSelectionProperties(props, new MysqlConnectorJTargetDriverDialect());

    assertEquals("primary", props.getProperty(TARGET_SERVER_TYPE));
  }
}
