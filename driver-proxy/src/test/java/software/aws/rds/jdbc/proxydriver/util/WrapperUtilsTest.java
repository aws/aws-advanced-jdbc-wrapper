/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.util;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.JdbcCallable;

public class WrapperUtilsTest {

  @Mock ConnectionPluginManager pluginManager;
  @Mock Object object;
  private AutoCloseable closeable;
  private int counter = 0;

  @BeforeEach
  private void init() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);

    doAnswer(invocation -> counter++).when(pluginManager).execute(
        any(Class.class),
        any(Class.class),
        any(Object.class),
        any(String.class),
        any(JdbcCallable.class),
        any(Object[].class));
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  Integer callExecuteWithPlugins() {
    return WrapperUtils.executeWithPlugins(
        Integer.class,
        pluginManager,
        object,
        "methodName",
        () -> 1);
  }

  Integer callExecuteWithPluginsWithException() {
    try {
      return WrapperUtils.executeWithPlugins(
          Integer.class,
          SQLException.class,
          pluginManager,
          object,
          "methodName",
          () -> 1);
    } catch (SQLException e) {
      fail();
    }

    return null;
  }

  @RepeatedTest(1000)
  void testExecutesWithPluginsIsSequential() {
    List<CompletableFuture<Integer>> futures = new ArrayList<>();
    List<Integer> results = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      futures.add(CompletableFuture.supplyAsync(this::callExecuteWithPlugins));
    }

    for (CompletableFuture<Integer> future : futures) {
      results.add(future.join());
    }

    Set<Integer> resultSet = new HashSet<>();
    for (Integer integer : results) {
      if (!resultSet.add(integer)) {
        fail();
      }
    }
  }

  @RepeatedTest(1000)
  void testExecutesWithPluginsWithExceptionIsSequential() {
    List<CompletableFuture<Integer>> futures = new ArrayList<>();
    List<Integer> results = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      futures.add(CompletableFuture.supplyAsync(this::callExecuteWithPluginsWithException));
    }

    for (CompletableFuture<Integer> future : futures) {
      results.add(future.join());
    }

    Set<Integer> resultSet = new HashSet<>();
    for (Integer integer : results) {
      if (!resultSet.add(integer)) {
        fail();
      }
    }
  }
}
