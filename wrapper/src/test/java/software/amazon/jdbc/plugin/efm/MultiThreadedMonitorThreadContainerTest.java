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

package software.amazon.jdbc.plugin.efm;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@Disabled
public class MultiThreadedMonitorThreadContainerTest {

  @Mock ExecutorServiceInitializer mockExecutorServiceInitializer;
  @Mock ExecutorService mockExecutorService;

  private AutoCloseable closeable;

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockExecutorServiceInitializer.createExecutorService()).thenReturn(mockExecutorService);
  }

  @AfterEach
  void cleanup() throws Exception {
    closeable.close();
    MonitorThreadContainer.releaseInstance();
  }

  @RepeatedTest(value = 1000, name = "MonitorThreadContainer ThreadPoolExecutor is not closed prematurely")
  void testThreadPoolExecutorNotClosedPrematurely() throws InterruptedException {
    MonitorThreadContainer.getInstance(mockExecutorServiceInitializer);

    ExecutorService executorService = Executors.newCachedThreadPool();
    executorService.execute(() -> MonitorThreadContainer.getInstance(mockExecutorServiceInitializer));
    Thread.sleep(3);
    executorService.execute(MonitorThreadContainer::releaseInstance);
    executorService.shutdown();

    verify(mockExecutorService, times(0)).shutdownNow();
  }
}
