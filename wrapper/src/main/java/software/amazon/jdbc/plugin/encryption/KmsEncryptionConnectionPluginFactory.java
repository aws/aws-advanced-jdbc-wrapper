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


package software.amazon.jdbc.plugin.encryption;

import java.util.logging.Logger;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.ConnectionPluginFactory;
import software.amazon.jdbc.PluginService;

import java.util.Properties;

/**
 * Factory for creating KmsEncryptionConnectionPlugin instances.
 * This factory is used by the AWS JDBC Wrapper to create plugin instances.
 */
public class KmsEncryptionConnectionPluginFactory implements ConnectionPluginFactory {

    private static final Logger LOGGER = Logger.getLogger(KmsEncryptionConnectionPluginFactory.class.getName());

    /**
     * Creates a new KmsEncryptionConnectionPlugin instance.
     *
     * @param pluginService The PluginService instance from AWS JDBC Wrapper
     * @param properties Configuration properties for the plugin
     * @return New plugin instance
     */
    @Override
    public ConnectionPlugin getInstance(PluginService pluginService, Properties properties) {
        LOGGER.info(()->"Creating KmsEncryptionConnectionPlugin instance");
        return new KmsEncryptionConnectionPlugin(pluginService, properties);
    }
}
