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

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUtils;

public class SamlUtils {
  private static final Pattern HTTPS_URL_PATTERN =
      Pattern.compile("^(https)://[-a-zA-Z0-9+&@#/%?=~_!:,.']*[-a-zA-Z0-9+&@#/%=~_']");

  final RdsUtils rdsUtils;

  public SamlUtils(final PluginService pluginService) {
    this(new RdsUtils());
  }

  SamlUtils(final RdsUtils rdsUtils) {
    this.rdsUtils = rdsUtils;
  }

  void checkIdpCredentialsWithFallback(
      final AwsWrapperProperty idpUserNameProperty,
      AwsWrapperProperty idpPasswordProperty,
      final Properties props) {
    if (idpUserNameProperty.getString(props) == null) {
      idpUserNameProperty.set(props, PropertyDefinition.USER.getString(props));
    }

    if (idpPasswordProperty.getString(props) == null) {
      idpPasswordProperty.set(props, PropertyDefinition.PASSWORD.getString(props));
    }
  }

  static void validateUrl(final String paramString) throws IOException {
    final URI authorizeRequestUrl = URI.create(paramString);
    final String errorMessage = Messages.get("AdfsCredentialsProviderFactory.invalidHttpsUrl",
        new Object[] {paramString});

    if (!authorizeRequestUrl.toURL().getProtocol().equalsIgnoreCase("https")) {
      throw new IOException(errorMessage);
    }

    final Matcher matcher = HTTPS_URL_PATTERN.matcher(paramString);
    if (!matcher.find()) {
      throw new IOException(errorMessage);
    }
  }

}
