package software.amazon.jdbc.plugin.bluegreen.routing;

import java.util.concurrent.TimeUnit;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenStatus;

public abstract class BaseRouting {

  protected long getNanoTime() {
    return System.nanoTime();
  }

  protected void delay(long delayMs, BlueGreenStatus bgStatus, PluginService pluginService, String bgdId)
      throws InterruptedException {

    long start = System.nanoTime();
    long end = start + TimeUnit.MILLISECONDS.toNanos(delayMs);
    final BlueGreenStatus currentStatus = bgStatus;
    long minDelay = Math.min(delayMs, 50);

    if (currentStatus == null) {
      TimeUnit.MILLISECONDS.sleep(delayMs);
    } else {
      // Check whether intervalType or stop flag change, or until waited specified delay time.
      do {
        synchronized (currentStatus) {
          currentStatus.wait(minDelay);
        }
      } while (
        // check if status reference is changed
          currentStatus == pluginService.getStatus(BlueGreenStatus.class, bgdId)
              && System.nanoTime() < end
              && !Thread.currentThread().isInterrupted());
    }
  }
}
