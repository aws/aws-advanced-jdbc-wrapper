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

package software.amazon.jdbc.plugin.bluegreen;

import java.util.HashMap;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

public enum BlueGreenRole {
  SOURCE(0),
  TARGET(1);

  // ver 1.0 mapping
  protected static final HashMap<String, BlueGreenRole> blueGreenRoleMapping_1_0 =
      new HashMap<String, BlueGreenRole>() {
        {
          put("BLUE_GREEN_DEPLOYMENT_SOURCE", BlueGreenRole.SOURCE);
          put("BLUE_GREEN_DEPLOYMENT_TARGET", BlueGreenRole.TARGET);
        }
      };

  private final int value;

  BlueGreenRole(final int newValue) {
    value = newValue;
  }

  public int getValue() {
    return value;
  }

  public static BlueGreenRole parseRole(final String value, final String version) {
    if ("1.0".equals(version)) {
      if (StringUtils.isNullOrEmpty(value)) {
        throw new IllegalArgumentException(Messages.get("bgd.unknownRole", new Object[] {value}));
      }
      final BlueGreenRole role = blueGreenRoleMapping_1_0.get(value.toUpperCase());

      if (role == null) {
        throw new IllegalArgumentException(Messages.get("bgd.unknownRole", new Object[] {value}));
      }
      return role;
    }
    throw new IllegalArgumentException(Messages.get("bgd.unknownVersion", new Object[] {version}));
  }

}
