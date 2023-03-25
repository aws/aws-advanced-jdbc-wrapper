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

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.JdbcCallable;

public class DriverMetaDataConnectionPlugin extends AbstractConnectionPlugin {

  public static final AwsWrapperProperty WRAPPER_DRIVER_NAME = new AwsWrapperProperty(
      "wrapperDriverName", "Amazon Web Services (AWS) Advanced JDBC Wrapper",
      "Override this value to return a specific driver name for the DatabaseMetaData#getDriverName method");

  private static final String GET_DRIVER_NAME = "DatabaseMetaData.getDriverName";
  private final Properties properties;

  public DriverMetaDataConnectionPlugin(Properties properties) {
    this.properties = properties;
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return new HashSet<>(Collections.singletonList(GET_DRIVER_NAME));
  }

  @Override
  public <T, E extends Exception> T execute(Class<T> resultClass, Class<E> exceptionClass,
      Object methodInvokeOn, String methodName, JdbcCallable<T, E> jdbcMethodFunc,
      Object[] jdbcMethodArgs) throws E {
    return (T) WRAPPER_DRIVER_NAME.getString(this.properties);
  }
}
