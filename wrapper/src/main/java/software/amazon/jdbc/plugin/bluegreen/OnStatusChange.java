package software.amazon.jdbc.plugin.bluegreen;

import java.util.List;
import java.util.Map;
import software.amazon.jdbc.HostSpec;

public interface OnStatusChange {
  void onStatusChanged(BlueGreenRole role, BlueGreenInterimStatus interimStatus);
}
