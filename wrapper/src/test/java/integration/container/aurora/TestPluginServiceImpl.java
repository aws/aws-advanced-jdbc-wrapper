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

package integration.container.aurora;

import java.sql.SQLException;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.PluginServiceImpl;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.FullServicesContainer;

public class TestPluginServiceImpl extends PluginServiceImpl {

  public TestPluginServiceImpl(
      @NonNull FullServicesContainer servicesContainer,
      @NonNull Properties props,
      @NonNull String originalUrl,
      String targetDriverProtocol,
      @NonNull final TargetDriverDialect targetDriverDialect)
      throws SQLException {

    super(servicesContainer, props, originalUrl, targetDriverProtocol, targetDriverDialect);
  }

  public static void clearHostAvailabilityCache() {
    PluginServiceImpl.hostAvailabilityExpiringCache.clear();
  }
}
