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

import java.util.Properties;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.ConnectionPluginFactory;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPlugin;

public class BlueGreenConnectionPluginFactory implements ConnectionPluginFactory {

  @Override
  public ConnectionPlugin getInstance(final PluginService pluginService, final Properties props) {
    return new BlueGreenConnectionPlugin(pluginService, props);
  }
}
