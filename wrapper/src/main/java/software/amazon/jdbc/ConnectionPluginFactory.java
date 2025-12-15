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

package software.amazon.jdbc;

import java.util.Properties;
import software.amazon.jdbc.util.FullServicesContainer;

/**
 * Interface for plugin factories. This class implements ways to initialize a plugin.
 */
public interface ConnectionPluginFactory {

  /**
   * Get an instance of a {@link ConnectionPlugin}.
   *
   * @param servicesContainer the service container containing the services to be used by the {@link ConnectionPlugin}.
   * @param props             to be used by the {@link ConnectionPlugin}.
   * @return an instance of a {@link ConnectionPlugin}.
   */
  ConnectionPlugin getInstance(FullServicesContainer servicesContainer, Properties props);
}
