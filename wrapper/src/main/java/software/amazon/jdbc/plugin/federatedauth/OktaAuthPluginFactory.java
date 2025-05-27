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

package software.amazon.jdbc.plugin.federatedauth;

import java.security.GeneralSecurityException;
import java.util.Properties;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.ConnectionPluginFactory;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.Messages;

public class OktaAuthPluginFactory implements ConnectionPluginFactory {

  @Override
  public ConnectionPlugin getInstance(PluginService pluginService, Properties props) {
    return new OktaAuthPlugin(pluginService, getCredentialsProviderFactory(pluginService, props));
  }

  private CredentialsProviderFactory getCredentialsProviderFactory(final PluginService pluginService,
      final Properties props) {
    return new OktaCredentialsProviderFactory(
        pluginService,
        () -> {
          try {
            return new HttpClientFactory().getCloseableHttpClient(
                OktaAuthPlugin.HTTP_CLIENT_SOCKET_TIMEOUT.getInteger(props),
                OktaAuthPlugin.HTTP_CLIENT_CONNECT_TIMEOUT.getInteger(props),
                OktaAuthPlugin.SSL_INSECURE.getBoolean(props));
          } catch (GeneralSecurityException e) {
            throw new RuntimeException(
                Messages.get("CredentialsProviderFactory.failedToInitializeHttpClient"), e);
          }
        });
  }
}
