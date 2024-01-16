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

package example.spring;

import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

public class RoutingDataSource extends AbstractRoutingDataSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(RoutingDataSource.class);

  private static final String WRITER = "writer";
  private static final String LOAD_BALANCED_READER = "load-balanced-reader";

  RoutingDataSource(DataSource primary, DataSource secondary) {

    Map<Object, Object> dataSources = new HashMap<>();
    dataSources.put(WRITER, primary);
    dataSources.put(LOAD_BALANCED_READER, secondary);

    setTargetDataSources(dataSources);
  }

  @Override
  protected Object determineCurrentLookupKey() {
    String dataSourceMode = LoadBalancedReaderDataSourceContext.isLoadBalancedReaderZone() ? LOAD_BALANCED_READER : WRITER;

    // Testing data source switch
    LOGGER.debug("Datasource: {}", dataSourceMode);

    return dataSourceMode;
  }

}
