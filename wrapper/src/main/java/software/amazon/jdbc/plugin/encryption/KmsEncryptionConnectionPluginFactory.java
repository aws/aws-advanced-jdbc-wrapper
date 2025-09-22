package software.amazon.jdbc.plugin.encryption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.ConnectionPluginFactory;
import software.amazon.jdbc.PluginService;

import java.util.Properties;

/**
 * Factory for creating KmsEncryptionConnectionPlugin instances.
 * This factory is used by the AWS JDBC Wrapper to create plugin instances.
 */
public class KmsEncryptionConnectionPluginFactory implements ConnectionPluginFactory {

    private static final Logger logger = LoggerFactory.getLogger(KmsEncryptionConnectionPluginFactory.class);

    /**
     * Creates a new KmsEncryptionConnectionPlugin instance.
     *
     * @param pluginService The PluginService instance from AWS JDBC Wrapper
     * @param properties Configuration properties for the plugin
     * @return New plugin instance
     */
    @Override
    public ConnectionPlugin getInstance(PluginService pluginService, Properties properties) {
        logger.info("Creating KmsEncryptionConnectionPlugin instance");
        return new KmsEncryptionConnectionPlugin(pluginService, properties);
    }
}
