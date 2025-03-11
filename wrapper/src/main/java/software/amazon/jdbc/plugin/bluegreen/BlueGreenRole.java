package software.amazon.jdbc.plugin.bluegreen;

public enum BlueGreenRole {
  SOURCE(0),
  TARGET(1);

  private final int value;

  BlueGreenRole(final int newValue) {
    value = newValue;
  }

  public int getValue() { return value; }
}
