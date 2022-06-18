/*
 *
 *  * AWS JDBC Proxy Driver
 *  * Copyright Amazon.com Inc. or affiliates.
 *  * See the LICENSE file in the project root for more information.
 *
 */

package software.aws.rds.jdbc.proxydriver.plugin;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.aws.rds.jdbc.proxydriver.ConnectionPlugin;
import software.aws.rds.jdbc.proxydriver.JdbcCallable;

public class ExecutionTimeConnectionPlugin extends AbstractConnectionPlugin implements ConnectionPlugin {

  private static final transient Logger LOGGER = Logger.getLogger(ExecutionTimeConnectionPlugin.class.getName());
  private static final Set<String> subscribedMethods = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("*")));

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public <T, E extends Exception> T execute(Class<T> resultClass, Class<E> exceptionClass,
      Object methodInvokeOn, String methodName, JdbcCallable<T, E> jdbcMethodFunc,
      Object[] jdbcMethodArgs) throws E {

    final long startTime = System.nanoTime();

    T result = jdbcMethodFunc.call();

    final long elapsedTime = (System.nanoTime() - startTime) / 1000000;
    LOGGER.log(Level.FINE, "Executed {0} in {1} ms", new Object[] {methodName, elapsedTime});

    return result;
  }
}
