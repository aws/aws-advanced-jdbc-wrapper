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

package example;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import software.amazon.jdbc.plugin.failover.FailoverSQLException;

@RestController
public class ApiController {

  @Autowired
  private ExampleService exampleService;

  @GetMapping(value = "/get")
  @Retryable(value = {FailoverSQLException.class}, maxAttempts = 3, backoff = @Backoff(delay = 5000))
  public List<Example> get() {
    return exampleService.get();
  }

  @GetMapping(value = "/add")
  @Retryable(value = {FailoverSQLException.class}, maxAttempts = 3, backoff = @Backoff(delay = 5000))
  public void add() {
    exampleService.add();
  }

  @GetMapping(value = "/delete")
  @Retryable(value = {FailoverSQLException.class}, maxAttempts = 3, backoff = @Backoff(delay = 5000))
  public void delete() {
    exampleService.delete();
  }
}
