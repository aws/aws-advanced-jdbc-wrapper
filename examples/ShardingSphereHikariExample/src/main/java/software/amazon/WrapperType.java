package software.amazon;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;

public final class WrapperType implements DatabaseType {

  @Override
  public Collection getJdbcUrlPrefixes() {
    return Collections.singleton("jdbc:aws-wrapper:mysql");
  }

  @Override
  public Optional getTrunkDatabaseType() {
    return Optional.of(TypedSPILoader.getService(DatabaseType.class, "MySQL"));
  }

  @Override
  public String getType() {
    return "Aurora MySQL";
  }
}
