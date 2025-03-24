package software.amazon.jdbc.plugin.bluegreen;

import java.util.HashMap;
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

  public int getValue() { return value; }

  public static BlueGreenRole parseRole(final String value, final String version) {
    if (StringUtils.isNullOrEmpty(value)) {
      throw new IllegalArgumentException("Unknown blue/green role " + value);
    }
    final BlueGreenRole role = blueGreenRoleMapping_1_0.get(value.toUpperCase());

    if (role == null) {
      throw new IllegalArgumentException("Unknown blue/green role " + value);
    }
    return role;
  }
}
