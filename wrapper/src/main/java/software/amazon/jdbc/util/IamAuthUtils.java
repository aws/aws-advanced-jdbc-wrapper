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

package software.amazon.jdbc.util;

import software.amazon.jdbc.HostSpec;

public class IamAuthUtils {
  public static String getIamHost(final String iamHost, final HostSpec hostSpec) {
    if (!StringUtils.isNullOrEmpty(iamHost)) {
      return iamHost;
    }
    return hostSpec.getHost();
  }

  public static int getIamPort(final int iamDefaultPort, final HostSpec hostSpec, final int dialectDefaultPort) {
    if (iamDefaultPort > 0) {
      return iamDefaultPort;
    } else if (hostSpec.isPortSpecified()) {
      return hostSpec.getPort();
    } else {
      return dialectDefaultPort;
    }
  }
}
