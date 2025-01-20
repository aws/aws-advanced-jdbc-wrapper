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

import integration.DatabaseEngine;
import integration.container.ContainerEnvironment;
import java.util.Arrays;
import java.util.logging.Logger;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class EnableOnDatabaseEngineCondition implements ExecutionCondition {

  private static final Logger LOGGER =
      Logger.getLogger(EnableOnDatabaseEngineCondition.class.getName());

  public EnableOnDatabaseEngineCondition() {}

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {

    final DatabaseEngine databaseEngine =
        ContainerEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine();

    boolean enabled =
        findAnnotation(context.getElement(), EnableOnDatabaseEngine.class)
            .map(
                annotation -> {
                  if (annotation == null || annotation.value() == null) {
                    return true;
                  }
                  return Arrays.stream(annotation.value()).anyMatch(v -> databaseEngine.equals(v));
                }) //
            .orElse(true);

    if (!enabled) {
      return ConditionEvaluationResult.disabled("Disabled by @EnableOnTestDatabaseEngine");
    }
    return ConditionEvaluationResult.enabled("Test enabled");
  }
}
