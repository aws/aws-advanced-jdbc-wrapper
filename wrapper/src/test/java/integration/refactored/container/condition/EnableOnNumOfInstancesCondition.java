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

package integration.refactored.container.condition;

import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;

import integration.refactored.container.TestEnvironment;
import java.util.logging.Logger;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

class EnableOnNumOfInstancesCondition implements ExecutionCondition {

  private static final Logger LOGGER =
      Logger.getLogger(EnableOnTestFeatureCondition.class.getName());

  public EnableOnNumOfInstancesCondition() {}

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {

    final int actualNumOfInstances =
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getInstances().size();

    boolean enabled =
        findAnnotation(context.getElement(), EnableOnNumOfInstances.class)
            .map(
                annotation -> {
                  if (annotation == null) {
                    return true;
                  }
                  if (annotation.min() > 0 && actualNumOfInstances < annotation.min()) {
                    return false;
                  }
                  if (annotation.max() > 0 && actualNumOfInstances > annotation.max()) {
                    return false;
                  }
                  return true;
                }) //
            .orElse(true);

    if (!enabled) {
      return ConditionEvaluationResult.disabled("Disabled by @EnableOnNumOfInstances");
    }
    return ConditionEvaluationResult.enabled("Test enabled");
  }
}
