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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
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
}
