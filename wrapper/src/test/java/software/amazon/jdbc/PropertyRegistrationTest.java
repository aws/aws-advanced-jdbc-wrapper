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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Every {@link AwsWrapperProperty} declared by the driver must be registered with
 * {@link PropertyDefinition}. An unregistered property still works for the wrapper, but its name is
 * not removed from the properties handed to the target driver, which then receives an unknown
 * property. The Okta plugin shipped that way once; this test keeps it from happening again.
 */
class PropertyRegistrationTest {

  @Test
  void everyDeclaredPropertyIsRegistered() throws IOException, ClassNotFoundException {
    final Path sourceRoot = locateMainJavaRoot();
    final List<Path> declaringSources;
    try (Stream<Path> files = Files.walk(sourceRoot)) {
      declaringSources = files
          .filter(p -> p.toString().endsWith(".java"))
          .filter(PropertyRegistrationTest::declaresAProperty)
          .collect(Collectors.toList());
    }
    assertFalse(declaringSources.isEmpty(), "No property declarations found under " + sourceRoot);

    final List<String> unregistered = new ArrayList<>();
    for (final Path source : declaringSources) {
      final String className = sourceRoot.relativize(source).toString()
          .replace(java.io.File.separatorChar, '.')
          .replaceAll("\\.java$", "");
      // Initializing the class runs its static registration block, as happens when the driver loads it.
      final Class<?> clazz = Class.forName(className, true, PropertyRegistrationTest.class.getClassLoader());
      for (final Field field : clazz.getDeclaredFields()) {
        if (field.getType() != AwsWrapperProperty.class
            || !Modifier.isPublic(field.getModifiers())
            || !Modifier.isStatic(field.getModifiers())) {
          continue;
        }
        final AwsWrapperProperty property;
        try {
          property = (AwsWrapperProperty) field.get(null);
        } catch (IllegalAccessException e) {
          throw new AssertionError("Could not read " + className + "." + field.getName(), e);
        }
        if (property != null && PropertyDefinition.byName(property.name) == null) {
          unregistered.add(className + "." + field.getName() + " (" + property.name + ")");
        }
      }
    }

    assertTrue(unregistered.isEmpty(),
        "These properties are declared but never registered with PropertyDefinition, so their names "
            + "are forwarded to the target driver. Add PropertyDefinition.registerPluginProperties(<Class>.class) "
            + "to a static block of the declaring class or its plugin:\n  " + String.join("\n  ", unregistered));
  }

  private static boolean declaresAProperty(final Path source) {
    try {
      return new String(Files.readAllBytes(source), StandardCharsets.UTF_8).contains("new AwsWrapperProperty(");
    } catch (IOException e) {
      throw new IllegalStateException("Could not read " + source, e);
    }
  }

  /** Resolves {@code wrapper/src/main/java} regardless of the working directory Gradle uses. */
  private static Path locateMainJavaRoot() {
    Path candidate = Paths.get("").toAbsolutePath();
    for (int i = 0; i < 5 && candidate != null; i++) {
      final Path wrapperSrc = candidate.resolve("wrapper").resolve("src").resolve("main").resolve("java");
      if (Files.isDirectory(wrapperSrc)) {
        return wrapperSrc;
      }
      final Path src = candidate.resolve("src").resolve("main").resolve("java");
      if (Files.isDirectory(src.resolve("software").resolve("amazon").resolve("jdbc"))) {
        return src;
      }
      candidate = candidate.getParent();
    }
    throw new IllegalStateException(
        "Could not locate wrapper/src/main/java from " + Paths.get("").toAbsolutePath());
  }
}
