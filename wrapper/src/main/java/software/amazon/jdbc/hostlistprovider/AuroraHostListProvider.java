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

package software.amazon.jdbc.hostlistprovider;


import java.util.logging.Logger;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.connection.ConnectionContext;


public class AuroraHostListProvider extends RdsHostListProvider {

  static final Logger LOGGER = Logger.getLogger(AuroraHostListProvider.class.getName());

  public AuroraHostListProvider(
      final ConnectionContext connectionContext,
      final FullServicesContainer servicesContainer,
      final String topologyQuery,
      final String nodeIdQuery,
      final String isReaderQuery) {
    super(
        connectionContext,
        servicesContainer,
        topologyQuery,
        nodeIdQuery,
        isReaderQuery);
  }
}
