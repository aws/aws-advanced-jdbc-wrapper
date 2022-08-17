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

package software.aws.jdbc.plugin;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.aws.jdbc.JdbcCallable;

public class ExecutionTimeConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER =
      Logger.getLogger(ExecutionTimeConnectionPlugin.class.getName());
  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<>(Collections.singletonList("*")));

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public <T, E extends Exception> T execute(
      Class<T> resultClass,
      Class<E> exceptionClass,
      Object methodInvokeOn,
      String methodName,
      JdbcCallable<T, E> jdbcMethodFunc,
      Object[] jdbcMethodArgs)
      throws E {

    final long startTime = System.nanoTime();

    T result = jdbcMethodFunc.call();

    final long elapsedTimeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
    LOGGER.log(Level.FINE, "Executed {0} in {1} ms", new Object[] {methodName, elapsedTimeMillis});

    return result;
  }
}
