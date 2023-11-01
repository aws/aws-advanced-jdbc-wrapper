package software.amazon.jdbc.plugin;

import java.util.Properties;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.ConnectionPluginFactory;
import software.amazon.jdbc.PluginService;

public class FederatedAuthConnectionPluginFactory implements ConnectionPluginFactory {
  @Override
  public ConnectionPlugin getInstance(final PluginService pluginService, final Properties props) {
    return new FederatedAuthConnectionPlugin(pluginService);
  }
}
