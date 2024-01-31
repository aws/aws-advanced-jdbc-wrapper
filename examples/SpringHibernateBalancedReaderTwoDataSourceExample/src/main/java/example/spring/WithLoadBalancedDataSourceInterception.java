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

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class WithLoadBalancedDataSourceInterception implements Ordered {

  private static final Logger LOGGER = LoggerFactory.getLogger(WithLoadBalancedDataSourceInterception.class);

  @Around("@annotation(example.spring.WithLoadBalancedReaderDataSource)")
  public Object aroundMethod(ProceedingJoinPoint joinPoint) throws Throwable {
    try {
      LoadBalancedReaderDataSourceContext.enter();
      //Debug data source switch
      LOGGER.debug("Entering load-balanced-reader dataSource zone");
      return joinPoint.proceed();
    } finally {
      LOGGER.debug("Leaving load-balanced-reader dataSource zone");
      LoadBalancedReaderDataSourceContext.exit();
    }
  }

  @Override
  public int getOrder() {
    // We'd like this interceptor to be handled with a higher priority than @Transactional.
    // This guarantees that datasource zone is defined before a new transaction starts.
    return Ordered.LOWEST_PRECEDENCE - 1;
  }
}
