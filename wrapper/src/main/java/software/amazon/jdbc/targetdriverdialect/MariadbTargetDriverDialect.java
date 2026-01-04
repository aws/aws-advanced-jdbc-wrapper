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

package software.amazon.jdbc.targetdriverdialect;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PropertyDefinition;

public class MariadbTargetDriverDialect extends GenericTargetDriverDialect {

  private static final String PERMIT_MYSQL_SCHEME = "permitMysqlScheme";
  private static final String DRIVER_CLASS_NAME = "org.mariadb.jdbc.Driver";
  private static final String DS_CLASS_NAME = "org.mariadb.jdbc.MariaDbDataSource";
  private static final String CP_DS_CLASS_NAME = "org.mariadb.jdbc.MariaDbPoolDataSource";

  private static final Set<String> MARIADB_ALLOWED_ON_CLOSED_METHOD_NAMES = Collections.unmodifiableSet(
      new HashSet<String>() {
        {
          addAll(ALLOWED_ON_CLOSED_METHODS);
          add(JdbcMethod.CONNECTION_GETCATALOG.methodName);
          add(JdbcMethod.CONNECTION_GETMETADATA.methodName);
          add(JdbcMethod.CONNECTION_ISREADONLY.methodName);
          add(JdbcMethod.CONNECTION_GETSCHEMA.methodName);
          add(JdbcMethod.CONNECTION_GETAUTOCOMMIT.methodName);
          add(JdbcMethod.CONNECTION_GETHOLDABILITY.methodName);
          add(JdbcMethod.CONNECTION_GETCLIENTINFO.methodName);
          add(JdbcMethod.CONNECTION_GETNETWORKTIMEOUT.methodName);
          add(JdbcMethod.CONNECTION_GETTYPEMAP.methodName);
          add(JdbcMethod.CONNECTION_CREATECLOB.methodName);
          add(JdbcMethod.CONNECTION_CREATEBLOB.methodName);
          add(JdbcMethod.CONNECTION_CREATENCLOB.methodName);
          add(JdbcMethod.CONNECTION_CLEARWARNINGS.methodName);
          add(JdbcMethod.CONNECTION_SETHOLDABILITY.methodName);
          add(JdbcMethod.CONNECTION_SETSCHEMA.methodName);
          add(JdbcMethod.STATEMENT_CLEARWARNINGS.methodName);
          add(JdbcMethod.STATEMENT_GETFETCHSIZE.methodName);
          add(JdbcMethod.STATEMENT_GETMAXFIELDSIZE.methodName);
          add(JdbcMethod.STATEMENT_GETRESULTSETTYPE.methodName);
          add(JdbcMethod.STATEMENT_ISCLOSEONCOMPLETION.methodName);
          add(JdbcMethod.STATEMENT_CLEARBATCH.methodName);
          add(JdbcMethod.STATEMENT_CLOSEONCOMPLETION.methodName);
          add(JdbcMethod.STATEMENT_GETGENERATEDKEYS.methodName);
          add(JdbcMethod.STATEMENT_GETMAXROWS.methodName);
          add(JdbcMethod.STATEMENT_GETMORERESULTS.methodName);
          add(JdbcMethod.STATEMENT_GETQUERYTIMEOUT.methodName);
          add(JdbcMethod.STATEMENT_GETRESULTSET.methodName);
          add(JdbcMethod.STATEMENT_GETRESULTSETCONCURRENCY.methodName);
          add(JdbcMethod.STATEMENT_GETUPDATECOUNT.methodName);
          add(JdbcMethod.STATEMENT_ADDBATCH.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETARRAY.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETBIGDECIMAL.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETBLOB.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETBOOLEAN.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETBYTE.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETBYTES.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETCHARACTERSTREAM.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETCLOB.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETDATE.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETDOUBLE.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETFLOAT.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETINT.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETLONG.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETNCHARACTERSTREAM.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETNCLOB.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETNSTRING.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETOBJECT.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETSHORT.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETTIME.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETSTRING.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETTIMESTAMP.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_GETURL.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_WASNULL.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_ADDBATCH.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_CLEARPARAMETERS.methodName);
        }
      });

  @Override
  public boolean isDialect(Driver driver) {
    return DRIVER_CLASS_NAME.equals(driver.getClass().getName());
  }

  @Override
  public boolean isDialect(String dataSourceClass) {
    return DS_CLASS_NAME.equals(dataSourceClass)
        || CP_DS_CLASS_NAME.equals(dataSourceClass);
  }

  @Override
  public ConnectInfo prepareConnectInfo(
      final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) {

    final String databaseName =
        PropertyDefinition.DATABASE.getString(props) != null
            ? PropertyDefinition.DATABASE.getString(props)
            : "";
    final boolean permitMysqlSchemeFlag = props.containsKey(PERMIT_MYSQL_SCHEME);

    // keep unknown properties (the ones that don't belong to AWS Wrapper Driver)
    // and use them to make a connection
    props.remove(PERMIT_MYSQL_SCHEME);
    PropertyDefinition.removeAllExcept(props,
        PropertyDefinition.USER.name,
        PropertyDefinition.PASSWORD.name,
        PropertyDefinition.TCP_KEEP_ALIVE.name,
        PropertyDefinition.CONNECT_TIMEOUT.name,
        PropertyDefinition.SOCKET_TIMEOUT.name);

    // "permitMysqlScheme" should be in Url rather than in properties.
    String urlBuilder = protocol + hostSpec.getUrl() + databaseName
        + (permitMysqlSchemeFlag ? "?" + PERMIT_MYSQL_SCHEME : "");

    return new ConnectInfo(urlBuilder, props);
  }

  @Override
  public void prepareDataSource(
      final @NonNull DataSource dataSource,
      final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException {

    // The logic is isolated to a separated class since it uses
    // direct reference to org.mariadb.jdbc.MariaDbDataSource
    final MariadbDriverHelper helper = new MariadbDriverHelper();
    helper.prepareDataSource(dataSource, protocol, hostSpec, props);
  }

  @Override
  public boolean isDriverRegistered() throws SQLException {
    final MariadbDriverHelper helper = new MariadbDriverHelper();
    return helper.isDriverRegistered();
  }

  @Override
  public void registerDriver() throws SQLException {
    final MariadbDriverHelper helper = new MariadbDriverHelper();
    helper.registerDriver();
  }

  @Override
  public Set<String> getAllowedOnConnectionMethodNames() {
    return MARIADB_ALLOWED_ON_CLOSED_METHOD_NAMES;
  }
}
