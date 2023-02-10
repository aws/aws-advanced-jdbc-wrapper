package software.amazon.jdbc.dialect;

import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.exceptions.ExceptionHandler;

public interface DatabaseDialect {
  public static final AwsWrapperProperty DATABASE_DIALECT =
      new AwsWrapperProperty(
          "databaseDialect",
          "false",
          "Set to true to automatically load-balance read-only transactions when setReadOnly is "
              + "set to true");

  public static DatabaseDialect getInstance() {
    return new DefaultDatabaseDialect();
  }
  public String getInstanceNameQuery();
  public String getInstanceNameColumn();
  public boolean isSupported();
  public int getDefaultPort();
  public ExceptionHandler getExceptionHandler();
  public String getTopologyQuery();
  public String getReadOnlyQuery();
  public String getReadOnlyColumnName();
  public String getHostPortQuery();
  public String getURLScheme();
}
