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

package example.data;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import example.spring.WithLoadBalancedReaderDataSource;
import example.spring.ShouldRetryTransactionException;

@Service
public class BookService {

  private static final Logger LOGGER = LoggerFactory.getLogger(BookService.class);

  @Autowired
  BookRepository repository;

  @Autowired
  ApplicationContext context;

  @Retryable(value = { ShouldRetryTransactionException.class, TransactionSystemException.class}, maxAttempts = 3)
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void updateBookAvailabilityTransactional() {

    LOGGER.debug(">>> start updateBookAvailabilityTransactional");
    LOGGER.debug("Current node: {}", this.repository.getNodeId());
    LOGGER.debug("Current sessionId: {}", this.repository.getNodeSessionId());

    final List<Book> allBooks = this.repository.findAll();
    LOGGER.info("books: {}", allBooks);
    for (Book book : allBooks) {
      book.setQuantityAvailable(book.getQuantityAvailable() + 1);
      LOGGER.info("Book '{}' new quantity: {}", book.getTitle(), book.getQuantityAvailable());
    }

    final BookService self = this.context.getBean(BookService.class);
    LOGGER.info("Number of books: {}", self.getNumOfBooksTransactional());

    /*
     The following statement is for testing purposes only.
     It allows enough time (2min) to initiate a manual cluster failover through AWS Console
     and observe how the application handles it.

     LOGGER.debug("before sleepQuery");
     this.repository.sleepQuery();
     LOGGER.debug("after sleepQuery");
    */

    /*
     After failover process in the driver is completed, all connections to an old writer node
     get forcibly closed. That can lead to a DataAccessResourceFailureException while executing
     the following statement. The exception is converted to ShouldRetryTransactionException in
     HibernateExceptionTranslator (Config.hibernateExceptionTranslator)
    */
    this.repository.saveAll(allBooks);

    // We want to make sure we're talking to the same writer node and use the same session.
    LOGGER.debug("Current node: {}", this.repository.getNodeId());
    LOGGER.debug("Current sessionId: {}", this.repository.getNodeSessionId());
    LOGGER.debug("<<< end updateBookAvailabilityTransactional");
  }

  @Retryable(value = { ShouldRetryTransactionException.class }, maxAttempts = 3)
  @WithLoadBalancedReaderDataSource
  @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
  public int getNumOfBooksTransactional() {
    LOGGER.debug(">>> start getNumOfBooksTransactional");
    LOGGER.debug("*Current node: {}", this.repository.getNodeId());
    LOGGER.debug("*Current sessionId: {}", this.repository.getNodeSessionId());

    int totalQuantity = this.repository.findAll().stream().mapToInt(Book::getQuantityAvailable).sum();

    /*
     The following statement is for testing purposes only.
     It gives enough time (2min) to initiate a manual cluster failover through AWS Console
     and observe how the application handles it. When failover process in the driver is completed,
     FailoverSQLException is raised. It's converted to ShouldRetryTransactionException in
     HibernateExceptionTranslator (see Config.hibernateExceptionTranslator)
    */
    LOGGER.debug("before sleepQuery");
    this.repository.sleepQuery();
    LOGGER.debug("after sleepQuery");

    // We want to make sure we're talking to the same reader node and use the same session.
    LOGGER.debug("*Current node: {}", this.repository.getNodeId());
    LOGGER.debug("*Current sessionId: {}", this.repository.getNodeSessionId());

    LOGGER.debug("<<< end getNumOfBooksTransactional");
    return totalQuantity;
  }
}
