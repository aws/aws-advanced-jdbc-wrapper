package software.amazon.jdbc.dialect;

import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.exceptions.GenericExceptionHandler;

public class DefaultDatabaseDialect implements DatabaseDialect{
  @Override
  public String getInstanceNameQuery() {
    return null;
  }

  @Override
  public String getInstanceNameColumn() {
    return null;
  }

  @Override
  public boolean isSupported() {
    return false;
  }

  @Override
  public int getDefaultPort() {
    return 0;
  }

  @Override
  public ExceptionHandler getExceptionHandler() {
    return new GenericExceptionHandler();
  }

  @Override
  public String getTopologyQuery() {
    return null;
  }

  @Override
  public String getReadOnlyQuery() {
    return null;
  }

  @Override
  public String getReadOnlyColumnName() {
    return null;
  }

  @Override
  public String getHostPortQuery() {
    return null;
  }
  public String getURLScheme() {
    return null;
  }

}
