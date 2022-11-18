package software.amazon.jdbc.osgi;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

public class WrapperBundleActivator implements BundleActivator {

  public void start(BundleContext context) throws Exception {
    if(!software.amazon.jdbc.Driver.isRegistered()) {
      software.amazon.jdbc.Driver.register();
    }
  }

  public void stop(BundleContext context) throws Exception {
    if (software.amazon.jdbc.Driver.isRegistered()) {
      software.amazon.jdbc.Driver.deregister();
    }
  }
}
