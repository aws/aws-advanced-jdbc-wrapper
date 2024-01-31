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

import org.springframework.dao.DataAccessException;

public class ShouldRetryTransactionException extends DataAccessException {

  public ShouldRetryTransactionException(String msg) {
    super(msg);
  }

  public ShouldRetryTransactionException(Throwable cause) {
    super("Transaction should be re-tried.", cause);
  }

  public ShouldRetryTransactionException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
