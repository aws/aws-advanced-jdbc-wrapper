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

package software.amazon.jdbc.plugin.efm.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class HostMonitoringPropertiesTest {

  @Test
  void create_appliesPrefixedOverrides_andDropsPrefixedNames() {
    final Properties source = new Properties();
    source.setProperty("user", "app");
    source.setProperty("socketTimeout", "30000");
    source.setProperty("monitoring-socketTimeout", "1000");
    source.setProperty("monitoring-user", "monitor");
    source.setProperty("monitoring-loginTimeout", "500");

    final Properties result = HostMonitoringProperties.create(source, null);

    assertEquals("monitor", result.getProperty("user"));
    assertEquals("1000", result.getProperty("socketTimeout"));
    assertEquals("500", result.getProperty("loginTimeout"));
    assertNull(result.getProperty("monitoring-socketTimeout"));
    assertNull(result.getProperty("monitoring-user"));
    assertNull(result.getProperty("monitoring-loginTimeout"));
  }

  @Test
  void create_doesNotModifyTheSource() {
    final Properties source = new Properties();
    source.setProperty("socketTimeout", "30000");
    source.setProperty("monitoring-socketTimeout", "1000");

    final Properties result = HostMonitoringProperties.create(source, null);

    assertNotSame(source, result);
    assertEquals("30000", source.getProperty("socketTimeout"));
    assertEquals("1000", source.getProperty("monitoring-socketTimeout"));
  }

  @Test
  void create_keepsUnprefixedPropertiesWithoutOverride() {
    final Properties source = new Properties();
    source.setProperty("connectTimeout", "10000");

    assertEquals("10000", HostMonitoringProperties.create(source, null).getProperty("connectTimeout"));
  }
}
