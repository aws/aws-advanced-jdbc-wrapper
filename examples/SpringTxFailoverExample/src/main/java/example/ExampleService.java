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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.support.RetrySynchronizationManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class ExampleService {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  @Autowired
  private ExampleDao exampleDao;

  public List<Example> get() {
    logger.info("Retry Number : {}", RetrySynchronizationManager.getContext().getRetryCount());
    List<Map<String, Object>> rows = exampleDao.getAll();
    List<Example> examples = new ArrayList<>();
    for (Map row : rows) {
      Example obj = new Example();
      obj.setId(((Integer) row.get("ID")));
      obj.setStatus((Integer) row.get("STATUS"));
      examples.add(obj);
    }
    return examples;
  }

  public void add() {
    logger.info("Retry Number : {}", RetrySynchronizationManager.getContext().getRetryCount());
    final Example example = new Example();
    example.setId(0);
    example.setStatus(0);
    exampleDao.create(example);
  }

  public void delete() {
    logger.info("Retry Number : {}", RetrySynchronizationManager.getContext().getRetryCount());
    exampleDao.delete();
  }
}
