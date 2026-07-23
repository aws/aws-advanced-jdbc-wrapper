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

package software.amazon.jdbc.plugin.readwritesplitting;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginService;

/** Unit tests for the endpoint-based Global Database plugins (assembly wiring / fail-fast). */
public class GdbSimpleReadWriteSplittingPluginTest {

  private AutoCloseable closeable;

  @Mock private PluginService pluginService;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  private Properties endpointProps() {
    final Properties props = new Properties();
    props.setProperty(SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.name, "writer.endpoint");
    props.setProperty(SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.name, "reader.endpoint");
    return props;
  }

  @Test
  void gdbSimple_missingWriteEndpoint_throwsAtConstruction() {
    final Properties props = new Properties();
    props.setProperty(SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.name, "reader.endpoint");
    assertThrows(RuntimeException.class,
        () -> new GdbSimpleReadWriteSplittingPlugin(pluginService, props));
  }

  @Test
  void gdbSimple_missingReadEndpoint_throwsAtConstruction() {
    final Properties props = new Properties();
    props.setProperty(SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.name, "writer.endpoint");
    assertThrows(RuntimeException.class,
        () -> new GdbSimpleReadWriteSplittingPlugin(pluginService, props));
  }

  @Test
  void gdbSimple_constructsAndSubscribesToSetReadOnly() {
    final GdbSimpleReadWriteSplittingPlugin plugin =
        new GdbSimpleReadWriteSplittingPlugin(pluginService, endpointProps());
    // setReadOnly-driven: subscribes to setReadOnly but not to statement-creation routing.
    assertTrue(plugin.getSubscribedMethods().contains(JdbcMethod.CONNECTION_SETREADONLY.methodName));
  }

  @Test
  void gdbAutoSimple_subscribesToPrepareMethodsForSqlRouting() {
    final GdbAutoSimpleReadWriteSplittingPlugin plugin =
        new GdbAutoSimpleReadWriteSplittingPlugin(pluginService, endpointProps());
    // SQL-routed: additionally subscribes to statement-creation methods.
    assertTrue(plugin.getSubscribedMethods().contains(JdbcMethod.CONNECTION_PREPARESTATEMENT.methodName));
    assertTrue(plugin.getSubscribedMethods().contains(JdbcMethod.CONNECTION_PREPARECALL.methodName));
  }
}
