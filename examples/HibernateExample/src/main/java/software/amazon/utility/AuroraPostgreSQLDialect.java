/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.utility;

import org.hibernate.PessimisticLockException;
import org.hibernate.QueryTimeoutException;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.dialect.PostgreSQLDriverKind;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.exception.LockAcquisitionException;
import org.hibernate.exception.spi.SQLExceptionConversionDelegate;
import org.hibernate.internal.util.JdbcExceptionHelper;

/*
The sole purpose of this class is to map our exceptions to new exceptions
for hibernate.
 */
public class AuroraPostgreSQLDialect extends PostgreSQLDialect {
    public AuroraPostgreSQLDialect(){
        super();
    }

    public AuroraPostgreSQLDialect(DialectResolutionInfo info) {
        super(info);
    }

    public AuroraPostgreSQLDialect(DatabaseVersion version) {
        super(version);
    }

    public AuroraPostgreSQLDialect(DatabaseVersion version, PostgreSQLDriverKind driverKind) {
        super(version, driverKind);
    }

    @Override
    public SQLExceptionConversionDelegate buildSQLExceptionConversionDelegate() {
        return (sqlException, message, sql) -> {
            switch ( JdbcExceptionHelper.extractSqlState( sqlException ) ) {
                case "40P01":
                    // DEADLOCK DETECTED
                    return new LockAcquisitionException(message, sqlException, sql);
                case "55P03":
                    // LOCK NOT AVAILABLE
                    return new PessimisticLockException(message, sqlException, sql);
                case "57014":
                    return new QueryTimeoutException( message, sqlException, sql );
                case "08001":
                    /* at this point we can't recover */
                    return new FailoverFailedException( message, sqlException, sql);
                case "08003":
                    /* tried to use a closed connection */
                    return new ConnectionClosedException( message, sqlException, sql);
                case "08007":
                    /* connection restarted during a transaction */
                    return new TransactionStateUnknownException(message, sqlException, sql);
                case "08S02":
                    /* connection restarted outside of a transaction */
                    return new ConnectionStateUnknownException(message, sqlException, sql);
                default:
                    // returning null allows other delegates to operate
                    return null;
            }
        };

    }
}
