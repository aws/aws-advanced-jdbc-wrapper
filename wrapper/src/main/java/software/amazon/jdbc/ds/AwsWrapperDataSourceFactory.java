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

package software.amazon.jdbc.ds;

import static software.amazon.jdbc.util.StringUtils.isNullOrEmpty;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import software.amazon.jdbc.util.PropertyUtils;

public class AwsWrapperDataSourceFactory implements ObjectFactory {
  @Override
  public Object getObjectInstance(
      final Object obj,
      final Name name,
      final Context nameCtx,
      final Hashtable<?, ?> environment)
      throws Exception {
    final List<String> dataSourcePropertyNames = Arrays.asList(
        "user",
        "password",
        "jdbcUrl",
        "targetDataSourceClassName",
        "jdbcProtocol",
        "serverPropertyName",
        "portPropertyName",
        "urlPropertyName",
        "databasePropertyName");

    final Reference reference = (Reference) obj;

    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    final List<Method> dsMethods = Arrays.asList(AwsWrapperDataSource.class.getMethods());
    for (final String dataSourceProperty : dataSourcePropertyNames) {
      final String referencePropertyContent = (String) reference.get(dataSourceProperty).getContent();
      if (!isNullOrEmpty(referencePropertyContent)) {
        PropertyUtils.setPropertyOnTarget(ds, dataSourceProperty, referencePropertyContent, dsMethods);
      }
    }

    final List<RefAddr> refAddrList = Collections.list(reference.getAll());
    final Properties props = new Properties();
    for (final RefAddr refAddr : refAddrList) {
      if (!dataSourcePropertyNames.contains(refAddr.getType())) {
        props.setProperty(refAddr.getType(), (String) refAddr.getContent());
      }
    }

    ds.setTargetDataSourceProperties(props);

    return ds;
  }
}
