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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.JdbcCallable;

public class WrapperUtilsTest {

  @Mock ConnectionPluginManager pluginManager;
  @Mock Object object;
  private AutoCloseable closeable;

  @BeforeEach
  @SuppressWarnings("unchecked")
  private void init() {
    final ReentrantLock pluginManagerLock = new ReentrantLock();
    final ReentrantLock testLock = new ReentrantLock();
    closeable = MockitoAnnotations.openMocks(this);

    doAnswer(invocation -> {
      pluginManagerLock.lock();
      return null;
    }).when(pluginManager).lock();
    doAnswer(invocation -> {
      pluginManagerLock.unlock();
      return null;
    }).when(pluginManager).unlock();

    doAnswer(invocation -> {
      boolean lockIsFree = testLock.tryLock();
      if (!lockIsFree) {
        fail("Lock is in use, should not be attempting to fetch it right now");
      }
      Thread.sleep(3000);
      testLock.unlock();
      return 1;
    }).when(pluginManager).execute(
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

  @Test
  void testExecutesWithPluginsIsSequential() {
    List<CompletableFuture<Integer>> futures = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      futures.add(CompletableFuture.supplyAsync(this::callExecuteWithPlugins));
    }

    for (CompletableFuture<Integer> future : futures) {
      future.join();
    }
  }

  @Test
  void testExecutesWithPluginsWithExceptionIsSequential() {
    List<CompletableFuture<Integer>> futures = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      futures.add(CompletableFuture.supplyAsync(this::callExecuteWithPluginsWithException));
    }

    for (CompletableFuture<Integer> future : futures) {
      future.join();
    }
  }
}
