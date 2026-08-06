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

package software.amazon.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Properties;
import org.junit.jupiter.api.Test;

public class AwsWrapperPropertyTest {

  @Test
  public void testNonNegativeRejectsNegativeInteger() {
    final AwsWrapperProperty prop =
        new AwsWrapperProperty("someTimeout", "1000", "desc").nonNegative();
    final Properties props = new Properties();
    props.setProperty("someTimeout", "-1");

    assertThrows(IllegalArgumentException.class, () -> prop.getInteger(props));
  }

  @Test
  public void testNonNegativeRejectsNegativeLong() {
    final AwsWrapperProperty prop =
        new AwsWrapperProperty("someTimeout", "1000", "desc").nonNegative();
    final Properties props = new Properties();
    props.setProperty("someTimeout", "-5");

    assertThrows(IllegalArgumentException.class, () -> prop.getLong(props));
  }

  @Test
  public void testNonNegativeAllowsZeroAndPositive() {
    final AwsWrapperProperty prop =
        new AwsWrapperProperty("someTimeout", "1000", "desc").nonNegative();
    final Properties props = new Properties();

    props.setProperty("someTimeout", "0");
    assertEquals(0, prop.getInteger(props));
    assertEquals(0L, prop.getLong(props));

    props.setProperty("someTimeout", "42");
    assertEquals(42, prop.getInteger(props));
    assertEquals(42L, prop.getLong(props));
  }

  @Test
  public void testNonNegativeUsesDefaultWhenUnset() {
    final AwsWrapperProperty prop =
        new AwsWrapperProperty("someTimeout", "1000", "desc").nonNegative();
    final Properties props = new Properties();

    assertEquals(1000, prop.getInteger(props));
  }

  @Test
  public void testNonNegativeValidatesTypedIntegerValue() {
    final AwsWrapperProperty prop =
        new AwsWrapperProperty("someTimeout", "1000", "desc").nonNegative();
    final Properties props = new Properties();
    props.put("someTimeout", -3);

    assertThrows(IllegalArgumentException.class, () -> prop.getInteger(props));
  }

  @Test
  public void testNonNegativeValidatesTypedLongValue() {
    final AwsWrapperProperty prop =
        new AwsWrapperProperty("someTimeout", "1000", "desc").nonNegative();
    final Properties props = new Properties();
    props.put("someTimeout", -3L);

    assertThrows(IllegalArgumentException.class, () -> prop.getLong(props));
  }

  @Test
  public void testWithBoundsEnforcesMinAndMax() {
    final AwsWrapperProperty prop =
        new AwsWrapperProperty("someValue", "5", "desc").withBounds(1L, 10L);
    final Properties props = new Properties();

    props.setProperty("someValue", "0");
    assertThrows(IllegalArgumentException.class, () -> prop.getInteger(props));

    props.setProperty("someValue", "11");
    assertThrows(IllegalArgumentException.class, () -> prop.getInteger(props));

    props.setProperty("someValue", "5");
    assertEquals(5, prop.getInteger(props));
  }

  @Test
  public void testUnboundedPropertyAcceptsNegative() {
    final AwsWrapperProperty prop = new AwsWrapperProperty("someValue", "-1", "desc");
    final Properties props = new Properties();
    props.setProperty("someValue", "-1");

    assertEquals(-1, prop.getInteger(props));
    assertEquals(-1L, prop.getLong(props));
  }
}
