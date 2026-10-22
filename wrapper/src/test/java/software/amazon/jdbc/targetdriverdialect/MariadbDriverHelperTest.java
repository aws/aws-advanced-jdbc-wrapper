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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.MariaDbDataSource;
import org.mariadb.jdbc.MariaDbPoolDataSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

public class MariadbDriverHelperTest {

  private static final String PROTOCOL = "jdbc:mariadb://";
  private static final Pattern CONNECT_TIMEOUT = Pattern.compile("[?&]connectTimeout=(\\d+)");

  private final MariadbDriverHelper helper = new MariadbDriverHelper();
  // Held in a field so the logger (and the handler attached to it) cannot be garbage-collected.
  private final Logger converterLogger = Logger.getLogger(TimeoutConverter.class.getName());
  private final List<LogRecord> warnings = Collections.synchronizedList(new ArrayList<>());
  private final Handler warningCollector = new Handler() {
    @Override
    public void publish(final LogRecord record) {
      if (record.getLevel() == Level.WARNING) {
        warnings.add(record);
      }
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
  };
  private Level previousConverterLevel;

  @BeforeEach
  void setUp() {
    previousConverterLevel = converterLogger.getLevel();
    converterLogger.setLevel(Level.ALL);
    converterLogger.addHandler(warningCollector);
    TimeoutConverter.clearReportedTruncatedValues();
  }

  @AfterEach
  void cleanUp() {
    converterLogger.removeHandler(warningCollector);
    converterLogger.setLevel(previousConverterLevel);
    TimeoutConverter.clearReportedTruncatedValues();
  }

  private static HostSpec host(final String host) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host(host).port(3306).build();
  }

  private static Properties props(final String... keyValues) {
    final Properties props = new Properties();
    for (int i = 0; i < keyValues.length; i += 2) {
      props.setProperty(keyValues[i], keyValues[i + 1]);
    }
    return props;
  }

  private static Integer urlConnectTimeout(final String url) {
    final Matcher matcher = CONNECT_TIMEOUT.matcher(url);
    return matcher.find() ? Integer.valueOf(matcher.group(1)) : null;
  }

  private MariaDbDataSource prepare(final Properties props) throws Exception {
    final MariaDbDataSource dataSource = new MariaDbDataSource();
    helper.prepareDataSource(dataSource, PROTOCOL, host("h"), props);
    return dataSource;
  }

  @Test
  void dataSource_loginTimeoutIsMilliseconds() throws Exception {
    final MariaDbDataSource dataSource = prepare(props("loginTimeout", "5000"));

    assertEquals(5000, urlConnectTimeout(dataSource.getUrl()), dataSource.getUrl());
    assertEquals(5, dataSource.getLoginTimeout());
    assertFalse(dataSource.getUrl().contains("loginTimeout"), dataSource.getUrl());
  }

  @Test
  void dataSource_connectTimeoutTakesPrecedenceOverLoginTimeout() throws Exception {
    final MariaDbDataSource dataSource = prepare(props("loginTimeout", "5000", "connectTimeout", "2000"));

    assertEquals(2000, urlConnectTimeout(dataSource.getUrl()), dataSource.getUrl());
  }

  @Test
  void dataSource_connectTimeoutAloneIsUnchanged() throws Exception {
    final MariaDbDataSource dataSource = prepare(props("connectTimeout", "5000"));

    assertEquals(5000, urlConnectTimeout(dataSource.getUrl()), dataSource.getUrl());
  }

  @Test
  void dataSource_wrapperTimeoutOverridesJdbcLoginTimeoutAlreadySet() throws Exception {
    // AwsWrapperDataSource calls setLoginTimeout(<JDBC login timeout>, 0 by default) on the target
    // data source before the helper runs. MariaDB applies that over the URL connectTimeout.
    final MariaDbDataSource withConnectTimeout = new MariaDbDataSource();
    withConnectTimeout.setLoginTimeout(0);
    helper.prepareDataSource(withConnectTimeout, PROTOCOL, host("h"), props("connectTimeout", "5000"));
    assertEquals(5000, urlConnectTimeout(withConnectTimeout.getUrl()), withConnectTimeout.getUrl());

    final MariaDbDataSource withLoginTimeout = new MariaDbDataSource();
    withLoginTimeout.setLoginTimeout(0);
    helper.prepareDataSource(withLoginTimeout, PROTOCOL, host("h"), props("loginTimeout", "5000"));
    assertEquals(5000, urlConnectTimeout(withLoginTimeout.getUrl()), withLoginTimeout.getUrl());
  }

  @Test
  void dataSource_subSecondValueIsTruncatedToZeroAndWarns() throws Exception {
    final MariaDbDataSource dataSource = prepare(props("loginTimeout", "500"));

    assertEquals(0, urlConnectTimeout(dataSource.getUrl()), dataSource.getUrl());
    assertEquals(0, dataSource.getLoginTimeout());
    assertEquals(1, warnings.size());
    final String message = warnings.get(0).getMessage();
    assertTrue(message.contains("'loginTimeout'"), message);
    assertTrue(message.contains("500"), message);
    assertTrue(message.contains("MariaDB Connector/J"), message);
    assertFalse(message.contains("{"), message);
  }

  @Test
  void dataSource_valueIsTruncatedToWholeSeconds() throws Exception {
    final MariaDbDataSource dataSource = prepare(props("connectTimeout", "2500"));

    assertEquals(2000, urlConnectTimeout(dataSource.getUrl()), dataSource.getUrl());
    assertTrue(warnings.isEmpty());
  }

  @Test
  void dataSource_subSecondConnectTimeoutOverridesUrlValueWithZero() throws Exception {
    final MariaDbDataSource dataSource = prepare(props("connectTimeout", "500"));

    assertEquals(0, urlConnectTimeout(dataSource.getUrl()), dataSource.getUrl());
    assertEquals(1, warnings.size());
    final String message = warnings.get(0).getMessage();
    assertTrue(message.contains("'connectTimeout'"), message);
  }

  @Test
  void dataSource_warnsOncePerPropertyAndValue() throws Exception {
    prepare(props("loginTimeout", "500"));
    prepare(props("loginTimeout", "500"));
    assertEquals(1, warnings.size());

    prepare(props("loginTimeout", "600"));
    assertEquals(2, warnings.size());
  }

  @Test
  void dataSource_zeroLoginTimeoutStaysZero() throws Exception {
    final MariaDbDataSource dataSource = prepare(props("loginTimeout", "0"));

    assertEquals(0, urlConnectTimeout(dataSource.getUrl()), dataSource.getUrl());
  }

  @Test
  void dataSource_noTimeoutPropertiesLeavesDriverDefault() throws Exception {
    final MariaDbDataSource dataSource = prepare(new Properties());

    assertNull(urlConnectTimeout(dataSource.getUrl()), dataSource.getUrl());
  }

  @Test
  void dataSource_reusedInstanceUsesLatestValue() throws Exception {
    final MariaDbDataSource dataSource = new MariaDbDataSource();
    helper.prepareDataSource(dataSource, PROTOCOL, host("h"), props("connectTimeout", "5000"));
    helper.prepareDataSource(dataSource, PROTOCOL, host("h2"), props("connectTimeout", "2000"));

    assertEquals(2000, urlConnectTimeout(dataSource.getUrl()), dataSource.getUrl());
  }

  @Test
  void poolDataSource_loginTimeoutIsMilliseconds() throws Exception {
    final MariaDbPoolDataSource dataSource = mock(MariaDbPoolDataSource.class);

    helper.prepareDataSource(dataSource, PROTOCOL, host("h"), props("loginTimeout", "5000"));

    final InOrder order = inOrder(dataSource);
    order.verify(dataSource).setLoginTimeout(5);
    final ArgumentCaptor<String> url = ArgumentCaptor.forClass(String.class);
    order.verify(dataSource).setUrl(url.capture());
    verify(dataSource, never()).setLoginTimeout(5000);
    assertFalse(url.getValue().contains("loginTimeout"), url.getValue());
  }

  @Test
  void poolDataSource_connectTimeoutTakesPrecedenceOverLoginTimeout() throws Exception {
    final MariaDbPoolDataSource dataSource = mock(MariaDbPoolDataSource.class);

    helper.prepareDataSource(dataSource, PROTOCOL, host("h"), props("loginTimeout", "5000", "connectTimeout", "2000"));

    verify(dataSource).setLoginTimeout(2);
    final ArgumentCaptor<String> url = ArgumentCaptor.forClass(String.class);
    verify(dataSource).setUrl(url.capture());
    assertTrue(url.getValue().contains("connectTimeout=2000"), url.getValue());
  }

  @Test
  void poolDataSource_connectTimeoutAlone() throws Exception {
    final MariaDbPoolDataSource dataSource = mock(MariaDbPoolDataSource.class);

    helper.prepareDataSource(dataSource, PROTOCOL, host("h"), props("connectTimeout", "5000"));

    verify(dataSource).setLoginTimeout(5);
  }

  @Test
  void poolDataSource_subSecondValueSetsZero() throws Exception {
    final MariaDbPoolDataSource dataSource = mock(MariaDbPoolDataSource.class);

    helper.prepareDataSource(dataSource, PROTOCOL, host("h"), props("connectTimeout", "999"));

    verify(dataSource).setLoginTimeout(0);
  }

  @Test
  void poolDataSource_noTimeoutPropertiesDoesNotSetLoginTimeout() throws Exception {
    final MariaDbPoolDataSource dataSource = mock(MariaDbPoolDataSource.class);

    helper.prepareDataSource(dataSource, PROTOCOL, host("h"), new Properties());

    verify(dataSource, never()).setLoginTimeout(anyInt());
    verify(dataSource).setUrl(anyString());
  }
}
