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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Guards the multi-release JDK 24 variant of {@link PgTargetDriverDialect} against drift.
 *
 * <p>A multi-release JAR replaces a class wholesale: on JDK 24 the copy under {@code src/main/java24}
 * is loaded instead of the one under {@code src/main/java}, not in addition to it. Both variants
 * extend {@code GenericTargetDriverDialect}, so a method present in the base variant but missing from
 * the JDK 24 variant does not fail to compile and does not disappear - it silently falls back to the
 * generic implementation. Several of those fallbacks are no-ops or plain-JDBC substitutes, which turns
 * an omission into a behaviour difference that only shows up at runtime on JDK 24. That has already
 * happened twice.
 *
 * <p>This test therefore asserts that the JDK 24 variant declares every method the base variant
 * declares. Overriding with different behaviour is fine and expected (for example
 * {@code abortConnection}, since the Security Manager was removed in JDK 24); omitting a method is
 * not.
 *
 * <p>The comparison is made on the sources rather than by reflection because only one of the two
 * classes can be on the classpath of a given JVM.
 */
public class PgTargetDriverDialectVariantTest {

  private static final String RELATIVE_PATH =
      "software/amazon/jdbc/targetdriverdialect/PgTargetDriverDialect.java";

  /**
   * Matches a method declaration and captures its name: an optional modifier list, a return type
   * (possibly generic, possibly an array, possibly annotated), the name, and an opening parenthesis.
   * Deliberately limited to {@code public} and {@code protected} members, which are the ones the
   * dialect contract is made of.
   */
  private static final Pattern METHOD_DECLARATION = Pattern.compile(
      "^\\s*(?:public|protected)\\s+"                 // visibility
          + "(?:static\\s+|final\\s+|synchronized\\s+)*"
          + "(?:@\\w+\\s+)*"                          // e.g. "byte @Nullable [] getEncryptedBytes"
          + "[\\w.<>,\\s\\[\\]?@]+?"                  // return type
          + "\\s(\\w+)\\s*\\(");                      // name + '('

  @Test
  void test_java24VariantDeclaresEveryBaseMethod() throws IOException {
    final Path sourceRoot = locateSourceRoot();
    final Set<String> baseMethods =
        declaredMethodNames(sourceRoot.resolve("main/java").resolve(RELATIVE_PATH));
    final Set<String> java24Methods =
        declaredMethodNames(sourceRoot.resolve("main/java24").resolve(RELATIVE_PATH));

    assertTrue(baseMethods.size() > 5,
        "The base variant should declare a meaningful number of methods, found: " + baseMethods);

    final Set<String> missing = new TreeSet<>(baseMethods);
    missing.removeAll(java24Methods);

    assertTrue(missing.isEmpty(),
        "The JDK 24 variant of PgTargetDriverDialect is missing methods declared by the base"
            + " variant, so on JDK 24 they silently fall back to GenericTargetDriverDialect: "
            + missing
            + ". Port them into wrapper/src/main/java24/" + RELATIVE_PATH
            + " (behaviour may differ, but the method must be declared).");
  }

  private static Set<String> declaredMethodNames(final Path source) throws IOException {
    assertTrue(Files.exists(source), "Expected to find source file " + source);

    final Set<String> names = new LinkedHashSet<>();
    final List<String> lines = Files.readAllLines(source, StandardCharsets.UTF_8);
    for (final String line : lines) {
      // Skip javadoc and comment bodies, which can otherwise look like declarations.
      final String trimmed = line.trim();
      if (trimmed.startsWith("*") || trimmed.startsWith("//") || trimmed.startsWith("/*")) {
        continue;
      }

      final Matcher matcher = METHOD_DECLARATION.matcher(line);
      if (matcher.find()) {
        final String name = matcher.group(1);
        // Constructors are named after the class and are not part of the dialect contract.
        if (!"PgTargetDriverDialect".equals(name)) {
          names.add(name);
        }
      }
    }

    return names;
  }

  /**
   * Resolves {@code wrapper/src} without depending on the working directory Gradle happens to use.
   */
  private static Path locateSourceRoot() {
    Path candidate = Paths.get("").toAbsolutePath();
    for (int i = 0; i < 5 && candidate != null; i++) {
      final Path wrapperSrc = candidate.resolve("wrapper").resolve("src");
      if (Files.isDirectory(wrapperSrc)) {
        return wrapperSrc;
      }
      final Path src = candidate.resolve("src");
      if (Files.isDirectory(src.resolve("main").resolve("java24"))) {
        return src;
      }
      candidate = candidate.getParent();
    }
    throw new IllegalStateException(
        "Could not locate the wrapper source root from " + Paths.get("").toAbsolutePath());
  }
}
