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

package integration.container.condition;

import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;

import integration.container.TestDriver;
import java.util.Arrays;
import java.util.logging.Logger;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class EnableBasedOnTestDriverExtension implements ExecutionCondition {

  private static final Logger LOGGER =
      Logger.getLogger(EnableBasedOnTestDriverExtension.class.getName());

  private final TestDriver testDriver;

  public EnableBasedOnTestDriverExtension(TestDriver testDriver) {
    this.testDriver = testDriver;
  }

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {

    if (!context.getElement().isPresent()) {
      return ConditionEvaluationResult.enabled("Test enabled");
    }

    boolean enabled =
        findAnnotation(context.getElement(), EnableOnTestDriver.class)
            .filter(annotation -> annotation != null && annotation.value() != null)
            .map(annotation -> Arrays.stream(annotation.value()).anyMatch(v -> v == testDriver))
            .orElse(true);

    boolean disabled =
        findAnnotation(context.getElement(), DisableOnTestDriver.class)
            .filter(annotation -> annotation != null && annotation.value() != null)
            .map(annotation -> Arrays.stream(annotation.value()).anyMatch(v -> v == testDriver))
            .orElse(!enabled);

    if (disabled) {
      LOGGER.finest("Disabled by @EnableOnTestDriver or @DisableOnTestDriver annotation.");
      return ConditionEvaluationResult.disabled(
          "Disabled by @EnableOnTestDriver or @DisableOnTestDriver annotation.");
    }

    LOGGER.finest("Test enabled");
    return ConditionEvaluationResult.enabled("Test enabled");
  }
}
