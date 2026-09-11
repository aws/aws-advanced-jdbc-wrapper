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

package software.amazon.jdbc.util.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.Topology;
import software.amazon.jdbc.util.events.BatchingEventPublisher;
import software.amazon.jdbc.util.events.DataAccessEvent;
import software.amazon.jdbc.util.events.DataInvalidationEvent;
import software.amazon.jdbc.util.events.Event;

/**
 * Covers the storage service's participation in the event bus. The invalidation path matters because
 * cached data outlives the component that produced it: keys such as a cluster id are reused by
 * replacement components, so data that is not discarded is handed to whoever comes next.
 */
class StorageServiceImplTest {

  private static final String KEY = "cluster-id";

  private AutoCloseable closeable;
  private TestPublisher publisher;
  private StorageServiceImpl storageService;

  /**
   * A publisher with no publishing thread, so delivery only happens when the test asks for it. The
   * accessors expose state that is protected on the base class and not reachable from this package.
   */
  private static class TestPublisher extends BatchingEventPublisher {

    @Override
    protected void initPublishingThread(long messageIntervalNanos) {
      // Do nothing.
    }

    boolean hasSubscribersFor(final Class<? extends Event> eventClass) {
      return this.subscribersMap.containsKey(eventClass);
    }

    boolean hasQueuedEvents() {
      return !this.eventMessages.isEmpty();
    }
  }

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    publisher = new TestPublisher();
    storageService = new TestStorageServiceImpl(publisher);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  private Topology topology() {
    final HostSpec host = new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host("some-instance").port(5432).build();
    return new Topology(Collections.singletonList(host));
  }

  @Test
  void storageServiceSubscribesToInvalidationOnConstruction() {
    assertTrue(publisher.hasSubscribersFor(DataInvalidationEvent.class),
        "The storage service must be subscribed before any item can be stored in it.");
  }

  @Test
  void publishedInvalidationDiscardsTheCachedItem() {
    storageService.set(KEY, topology());
    assertNotNull(storageService.get(Topology.class, KEY));

    publisher.publish(new DataInvalidationEvent(Topology.class, KEY));

    assertNull(storageService.get(Topology.class, KEY),
        "A published invalidation must discard the cached item.");
  }

  /**
   * Batched delivery would leave the stale item readable until the next batch is sent, which for the
   * default publisher interval is up to 30 seconds. The event therefore has to be delivered on the
   * publishing call itself, without any batch being flushed.
   */
  @Test
  void invalidationIsDeliveredImmediatelyRatherThanBatched() {
    storageService.set(KEY, topology());

    publisher.publish(new DataInvalidationEvent(Topology.class, KEY));

    assertFalse(publisher.hasQueuedEvents(),
        "An invalidation must not be queued for a later batch.");
    assertNull(storageService.get(Topology.class, KEY),
        "The item must already be gone before any batch is flushed.");
  }

  @Test
  void invalidationOnlyAffectsTheGivenKey() {
    storageService.set(KEY, topology());
    storageService.set("other-cluster", topology());

    publisher.publish(new DataInvalidationEvent(Topology.class, KEY));

    assertNull(storageService.get(Topology.class, KEY));
    assertNotNull(storageService.get(Topology.class, "other-cluster"),
        "Invalidation is per key and must not clear the whole item class.");
  }

  @Test
  void invalidatingAnAbsentItemIsHarmless() {
    // No item stored, and the item class has never been used. Delivery must not throw, otherwise
    // BatchingEventPublisher would stop notifying the remaining subscribers of the same event.
    publisher.publish(new DataInvalidationEvent(Topology.class, "never-stored"));

    assertEquals(0, storageService.size(Topology.class));
  }

  @Test
  void unrelatedEventsAreIgnored() {
    storageService.set(KEY, topology());

    storageService.processEvent(new DataAccessEvent(Topology.class, KEY));

    assertNotNull(storageService.get(Topology.class, KEY),
        "Only invalidation events may remove cached data.");
  }

  /**
   * The regression this guards: this service is a long-lived singleton that
   * {@code Driver.releaseResources()} releases without replacing. If releasing dropped the
   * subscription, invalidation would be permanently dead for a driver that is used again afterwards,
   * and stale cached data would silently reappear.
   */
  @Test
  void invalidationStillWorksAfterReleasingResources() {
    storageService.releaseResources();

    assertTrue(publisher.hasSubscribersFor(DataInvalidationEvent.class),
        "Releasing resources must not drop the invalidation subscription.");

    storageService.set(KEY, topology());
    publisher.publish(new DataInvalidationEvent(Topology.class, KEY));

    assertNull(storageService.get(Topology.class, KEY));
  }
}
