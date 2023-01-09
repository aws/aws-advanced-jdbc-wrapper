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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.junit.jupiter.api.Test;

public class PropertyDefinitionTests {

  @Test
  public void testRemoveAllExceptCredentials() {

    final String expectedKeyToBeRemoved = PropertyDefinition.DATABASE.name;
    final String expectedOtherKeyToBeRemoved = PropertyDefinition.PROFILE_NAME.name;
    final String expectedKeyToRemain = "someKeyToRemain";
    final Properties props = new Properties();
    props.setProperty(PropertyDefinition.USER.name, "someUsername");
    props.setProperty(PropertyDefinition.PASSWORD.name, "somePassword");
    props.setProperty(expectedKeyToBeRemoved, "someValue");
    props.setProperty(expectedOtherKeyToBeRemoved, "someOtherValue");
    props.setProperty(expectedKeyToRemain, "someValue");
    PropertyDefinition.removeAllExceptCredentials(props);

    assertFalse(props.containsKey(expectedKeyToBeRemoved));
    assertFalse(props.containsKey(expectedOtherKeyToBeRemoved));
    assertTrue(props.containsKey(PropertyDefinition.USER.name));
    assertTrue(props.containsKey(PropertyDefinition.PASSWORD.name));
    assertTrue(props.containsKey(expectedKeyToRemain));
    assertEquals(3, props.size());
  }

  @Test
  public void testRemoveAllExceptCredentialsGivenNoUserProperty() {
    final String expectedKeyToBeRemoved = PropertyDefinition.DATABASE.name;
    final String expectedOtherKeyToBeRemoved = PropertyDefinition.PROFILE_NAME.name;
    final Properties props = new Properties();
    props.setProperty(PropertyDefinition.PASSWORD.name, "somePassword");
    props.setProperty(expectedKeyToBeRemoved, "someValue");
    props.setProperty(expectedOtherKeyToBeRemoved, "someOtherValue");

    PropertyDefinition.removeAllExceptCredentials(props);
    assertTrue(props.containsKey(PropertyDefinition.PASSWORD.name));
    assertEquals(1, props.size());
  }

  @Test
  public void testRemoveAllExceptCredentialsGivenNoPasswordProperty() {
    final String expectedKeyToBeRemoved = PropertyDefinition.DATABASE.name;
    final String expectedOtherKeyToBeRemoved = PropertyDefinition.PROFILE_NAME.name;
    final Properties props = new Properties();
    props.setProperty(PropertyDefinition.USER.name, "someUser");
    props.setProperty(expectedKeyToBeRemoved, "someValue");
    props.setProperty(expectedOtherKeyToBeRemoved, "someOtherValue");

    PropertyDefinition.removeAllExceptCredentials(props);
    assertTrue(props.containsKey(PropertyDefinition.USER.name));
    assertEquals(1, props.size());
  }

  @Test
  public void testRemoveAllExceptCredentialsGivenNoUserAndPasswordProperty() {
    final String expectedKeyToBeRemoved = PropertyDefinition.DATABASE.name;
    final String expectedOtherKeyToBeRemoved = PropertyDefinition.PROFILE_NAME.name;
    final Properties props = new Properties();
    props.setProperty(expectedKeyToBeRemoved, "someValue");
    props.setProperty(expectedOtherKeyToBeRemoved, "someOtherValue");

    PropertyDefinition.removeAllExceptCredentials(props);

    assertTrue(props.isEmpty());
  }
}
