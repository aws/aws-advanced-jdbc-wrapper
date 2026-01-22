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

package software.amazon.jdbc.plugin;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.JdbcCallable;

class ExecutionTimeConnectionPluginTest {
  private AutoCloseable closeable;

  @Mock Statement mockStatement;
  @Mock JdbcCallable<ResultSet, SQLException> mockCallable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  void test_executeTime() throws SQLException, UnsupportedEncodingException {
    when(mockCallable.call()).thenAnswer(i -> {
      TimeUnit.MILLISECONDS.sleep(10);
      return null;
    });
    final Logger logger = Logger.getLogger(""); // get root logger
    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    final StreamHandler handler = new StreamHandler(os, new SimpleFormatter());
    handler.setLevel(Level.ALL);
    logger.addHandler(handler);
    logger.setLevel(Level.ALL);

    final Logger packageLogger = Logger.getLogger("software.amazon.jdbc");
    packageLogger.setLevel(Level.ALL);

    final ExecutionTimeConnectionPlugin plugin = new ExecutionTimeConnectionPlugin();

    plugin.execute(
        ResultSet.class,
        SQLException.class,
        mockStatement,
        "Statement.executeQuery",
        mockCallable,
        new Object[] {});

    handler.flush();
    String logMessages = os.toString("UTF-8");

    assertTrue(logMessages.contains("Executed Statement.executeQuery in"));
  }
}
