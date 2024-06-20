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

package software.amazon.jdbc.plugin.bluegreen;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

public class BlueGreenStatusSimulator {

  private static final Logger LOGGER = Logger.getLogger(BlueGreenStatusSimulator.class.getName());

  private static long initTime;
  private static long switchoverDelayNano;
  private static long switchoverCompleteDelayNano;
  private static long switchoverTimeNano;
  private static long switchoverCompleteTimeNano;
  private static final AtomicReference<BlueGreenPhases> currentPhase = new AtomicReference<>(BlueGreenPhases.CREATED);

  private static final ExecutorService executorService = Executors.newFixedThreadPool(1);

  public static void init(final long switchOverDelay, final long switchoverCompleteDelay) {

    switchoverDelayNano = switchOverDelay;
    switchoverCompleteDelayNano = switchoverCompleteDelay;
    reset();
    executorService.submit(() -> {
      while (System.nanoTime() <= switchoverCompleteTimeNano) {
        if (System.nanoTime() >= switchoverTimeNano) {
          if (currentPhase.get() != BlueGreenPhases.SWITCHING_OVER) {
            currentPhase.set(BlueGreenPhases.SWITCHING_OVER);
            LOGGER.info("================= " + BlueGreenPhases.SWITCHING_OVER);
          }
        }
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      currentPhase.set(BlueGreenPhases.SWITCH_OVER_COMPLETED);
      LOGGER.info("================= " + BlueGreenPhases.SWITCH_OVER_COMPLETED);
    });
    executorService.shutdown();
  }

  public static BlueGreenPhases getPhase() {
    return currentPhase.get();
  }

  public static void reset() {
    initTime = System.nanoTime();
    switchoverTimeNano = initTime + switchoverDelayNano;
    switchoverCompleteTimeNano = initTime + switchoverCompleteDelayNano;
  }
}
