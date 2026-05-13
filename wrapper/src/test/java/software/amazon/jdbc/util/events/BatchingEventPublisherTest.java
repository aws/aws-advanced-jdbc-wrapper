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

package software.amazon.jdbc.util.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.plugin.customendpoint.CustomEndpointInfo;

class BatchingEventPublisherTest {
  private AutoCloseable closeable;
  @Mock private EventSubscriber subscriber;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  public void testPublication() {
    BatchingEventPublisher publisher = new BatchingEventPublisher() {
      @Override
      protected void initPublishingThread(long messageIntervalNanos) {
        // Do nothing
      }
    };

    Set<Class<? extends Event>> eventSubscriptions = new HashSet<>(Collections.singletonList(DataAccessEvent.class));
    publisher.subscribe(subscriber, eventSubscriptions);
    publisher.subscribe(subscriber, eventSubscriptions);
    assertEquals(1, publisher.subscribersMap.size());

    DataAccessEvent event = new DataAccessEvent(CustomEndpointInfo.class, "key");
    publisher.publish(event);
    publisher.publish(event);
    publisher.sendMessages();
    assertTrue(publisher.eventMessages.isEmpty());

    verify(subscriber, times(1)).processEvent(eq(event));

    publisher.unsubscribe(subscriber, eventSubscriptions);
    publisher.publish(event);
    publisher.sendMessages();
    assertTrue(publisher.eventMessages.isEmpty());
    verify(subscriber, times(1)).processEvent(eq(event));
  }

  @Test
  public void testReleaseResources_shutsDownExecutor() {
    BatchingEventPublisher publisher = new BatchingEventPublisher() {
      @Override
      protected void initPublishingThread(long messageIntervalNanos) {
        // Do nothing
      }
    };

    // Release resources should not throw
    BatchingEventPublisher.releaseResources();

    // The static executor should be shut down
    assertTrue(BatchingEventPublisher.publishingExecutor.isShutdown());
  }

  @Test
  public void testExecutorRecreatedAfterReleaseResources() {
    // Release resources to shut down the executor
    BatchingEventPublisher.releaseResources();
    assertTrue(BatchingEventPublisher.publishingExecutor.isShutdown());

    // Creating a new publisher should recreate the executor
    BatchingEventPublisher publisher = new BatchingEventPublisher() {
      @Override
      protected void initPublishingThread(long messageIntervalNanos) {
        // Call super to trigger executor recreation
        super.initPublishingThread(messageIntervalNanos);
      }
    };

    // Executor should be recreated and running
    assertFalse(BatchingEventPublisher.publishingExecutor.isShutdown());
  }

  @Test
  public void testPublishAfterReleaseResources() throws InterruptedException {
    // Release resources to shut down the executor
    BatchingEventPublisher.releaseResources();
    assertTrue(BatchingEventPublisher.publishingExecutor.isShutdown());

    // Create a publisher with a short interval for testing
    final long shortInterval = TimeUnit.MILLISECONDS.toNanos(100);
    final CountDownLatch latch = new CountDownLatch(1);

    BatchingEventPublisher publisher = new BatchingEventPublisher(shortInterval);

    Set<Class<? extends Event>> eventSubscriptions = new HashSet<>(Collections.singletonList(DataAccessEvent.class));
    publisher.subscribe(new EventSubscriber() {
      @Override
      public void processEvent(Event event) {
        latch.countDown();
      }
    }, eventSubscriptions);

    // Publish an event - this should trigger executor recreation via ensureExecutorRunning
    DataAccessEvent event = new DataAccessEvent(CustomEndpointInfo.class, "key");
    publisher.publish(event);

    // Wait for the scheduled task to deliver the event
    assertTrue(latch.await(2, TimeUnit.SECONDS), "Event should have been delivered after executor recreation");
  }

  @Test
  public void testMultipleReleaseResourcesCalls() {
    // Calling releaseResources multiple times should not throw
    BatchingEventPublisher.releaseResources();
    BatchingEventPublisher.releaseResources();
    BatchingEventPublisher.releaseResources();

    // Should still be able to create a publisher after multiple releases
    BatchingEventPublisher publisher = new BatchingEventPublisher();
    assertFalse(BatchingEventPublisher.publishingExecutor.isShutdown());
  }
}
