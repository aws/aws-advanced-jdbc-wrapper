package software.amazon.jdbc.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;

class ResourceLockTest {

    @Test
    void testObtainClose() {
        final ResourceLock lock = new ResourceLock();

        assertFalse(lock.isLocked());
        assertFalse(lock.isHeldByCurrentThread());

        try (ResourceLock ignore = lock.obtain()) {
            assertTrue(lock.isLocked());
            assertTrue(lock.isHeldByCurrentThread());
        }

        assertFalse(lock.isLocked());
        assertFalse(lock.isHeldByCurrentThread());
    }

    @Test
    void testObtainWhenMultiThreadedExpectLinearExecution() throws InterruptedException, ExecutionException {
        CallWithResourceLock callWithResourceLock = new CallWithResourceLock();

        int levelOfConcurrency = 5;

        ExecutorService executorService = Executors.newFixedThreadPool(levelOfConcurrency);
        try {
            List<Callable<CounterPair>> callables = new ArrayList<>();
            for (int i = 0; i < levelOfConcurrency; i++) {
                callables.add(callWithResourceLock::invoke);
            }

            // expect linear execution
            List<Future<CounterPair>> results = executorService.invokeAll(callables);

            Set<Integer> preLockSet = new HashSet<>();
            Set<Integer> postLockSet = new HashSet<>();
            for (Future<CounterPair> result : results) {
                CounterPair entry = result.get();
                preLockSet.add(entry.preLock);
                postLockSet.add(entry.postLock);
            }

            assertEquals(levelOfConcurrency, postLockSet.size()); // linear execution inside resource lock block
            assertEquals(1, preLockSet.size()); // all threads called invoke before any finish

        } finally {
            executorService.shutdown();
        }
    }

    static final class CallWithResourceLock {

        // wait enough time to allow concurrent threads to block on the lock
        final long waitTime = TimeUnit.MILLISECONDS.toNanos(20);
        final ResourceLock lock = new ResourceLock();
        final AtomicInteger counter = new AtomicInteger();

        /**
         * Invoke returning 'pre lock' and 'post lock' counter value.
         */
        CounterPair invoke() {
            int preLock = counter.get();
            try (ResourceLock ignore = lock.obtain()) {
                int postLock = counter.get();
                LockSupport.parkNanos(waitTime);
                counter.incrementAndGet();
                return new CounterPair(preLock, postLock);
            }
        }
    }

    static final class CounterPair {
        final int preLock;
        final int postLock;

        CounterPair(int preLock, int postLock) {
            this.preLock = preLock;
            this.postLock = postLock;
        }
    }

}