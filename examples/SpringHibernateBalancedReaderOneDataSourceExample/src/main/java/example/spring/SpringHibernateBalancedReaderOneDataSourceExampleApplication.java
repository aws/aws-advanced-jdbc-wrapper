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

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.retry.annotation.EnableRetry;
import example.data.BookService;

@SpringBootApplication
@EnableJpaRepositories(basePackages = {"example.data"}, enableDefaultTransactions = false)
@EntityScan("example.data")
@EnableRetry
public class SpringHibernateBalancedReaderOneDataSourceExampleApplication implements CommandLineRunner {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(SpringHibernateBalancedReaderOneDataSourceExampleApplication.class);

  @Autowired
  BookService bookService;

  @Override
  public void run(String... args) throws InterruptedException {
    runSingleThread();
    //runMultiThread();
  }

  // The method helps to observe logical transactions and associated connections.
  public void runSingleThread() {
    int failedTransactionCount = 0;
    for (int i = 1; i <= 10000; i++) {
      LOGGER.info("Iteration: {}", i);
      try {
        bookService.updateBookAvailabilityTransactional();
      } catch (Exception ex) {
        failedTransactionCount++;
      }
      LOGGER.debug("Failed transactions: " + failedTransactionCount);
    }
  }

  /**
   * The method helps to execute a simple logic updating number of available books in multiple threads.
   * If a failover manually triggered through AWS Console, it's possible to observe how the application
   * handles the failover and check if there's any failed transactions there.
   */
  public void runMultiThread() throws InterruptedException {
    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicInteger completed = new AtomicInteger(0);
    final AtomicInteger failedTransactionCount = new AtomicInteger(0);
    final Executor executor = Executors.newFixedThreadPool(250);
    for (int i = 1; i <= 250; i++) {
      final int threadNum = i;
      executor.execute(() -> {
        try {
          for (int k = 1; k <= 2500; k++) {
            if (stop.get()) {
              break;
            }
            try {
              bookService.updateBookAvailabilityTransactional();
            } catch (Exception ex) {
              failedTransactionCount.incrementAndGet();
            }
            LOGGER.debug("Failed transactions: " + failedTransactionCount.get());
          }
        } finally {
          completed.incrementAndGet();
          LOGGER.debug("Thread " + threadNum + " is completed. Total completed: " + completed.get());
          LOGGER.debug("Failed transactions: " + failedTransactionCount.get());
        }
      });
    }

    TimeUnit.MINUTES.sleep(30);
    stop.set(true);

    LOGGER.debug("Failed transactions: " + failedTransactionCount.get());
  }

  public static void main(String[] args) {
    new SpringApplicationBuilder(SpringHibernateBalancedReaderOneDataSourceExampleApplication.class)
        .web(WebApplicationType.NONE)
        .run(args);
  }
}
