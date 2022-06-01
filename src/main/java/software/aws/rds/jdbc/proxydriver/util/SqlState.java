/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.util;

import org.checkerframework.checker.nullness.qual.Nullable;

public class SqlState {

    public static final SqlState UNKNOWN_STATE = new SqlState("");

    //TODO: add custom error codes support

    private final String sqlState;

    SqlState(String sqlState) {
        this.sqlState = sqlState;
    }

    public String getCode() {
        return this.sqlState;
    }

    public static boolean isConnectionError(@Nullable String psqlState) {
        //TODO
        return false;
    }
}
