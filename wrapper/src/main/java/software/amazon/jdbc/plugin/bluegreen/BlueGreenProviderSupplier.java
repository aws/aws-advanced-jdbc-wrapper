package software.amazon.jdbc.plugin.bluegreen;

import java.util.Properties;
import software.amazon.jdbc.PluginService;

public interface BlueGreenProviderSupplier {

  BlueGreenStatusProvider create(PluginService pluginService, Properties props, String bgdId);
}
