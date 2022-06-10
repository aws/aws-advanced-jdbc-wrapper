package software.aws.rds.jdbc.proxydriver;

import java.sql.SQLException;

public interface HostListProvider {
    void refresh() throws SQLException;
}
