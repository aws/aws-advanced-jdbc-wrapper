package software.amazon.jdbc.plugin.dev;

public class ExceptionSimulatorManager {

  static Throwable nextException;
  static ExceptionSimulatorConnectCallback connectCallback;

  public static void raiseExceptionOnNextConnect(final Throwable throwable) {
    nextException = throwable;
  }

  public static void setCallback(final ExceptionSimulatorConnectCallback exceptionSimulatorConnectCallback) {
    connectCallback = exceptionSimulatorConnectCallback;
  }
}
