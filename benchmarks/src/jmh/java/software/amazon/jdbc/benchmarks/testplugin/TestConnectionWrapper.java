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

package software.amazon.jdbc.benchmarks.testplugin;

import java.sql.SQLException;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

/**
 * Test class allowing mocks to be used with {@link ConnectionWrapper} logic.
 *
 * <p>This delegates to the production constructor and supplies a (mocked) services container
 * rather than using the {@code protected} "for testing purposes only" constructor. That
 * constructor leaves {@code ConnectionWrapper.servicesContainer} null, and every JDBC call now
 * routes through {@code WrapperUtils.doExecuteWithPlugins}, which dereferences
 * {@code getServicesContainer()}. Going through the production constructor keeps the benchmarks
 * aligned with the code path real connections take.
 */
public class TestConnectionWrapper extends ConnectionWrapper {
  public TestConnectionWrapper(
      @NonNull final FullServicesContainer servicesContainer,
      @NonNull final Properties props,
      @NonNull final String url,
      @NonNull final String protocol)
      throws SQLException {
    super(servicesContainer, props, url, protocol, null);
  }
}
