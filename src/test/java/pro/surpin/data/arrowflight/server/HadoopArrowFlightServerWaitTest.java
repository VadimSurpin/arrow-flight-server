package pro.surpin.data.arrowflight.server;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests bounded cluster membership condition waiting. */
@Tag("unit")
class HadoopArrowFlightServerWaitTest {

    /** Verifies a member event observed before waiting cannot be lost. */
    @Test
    void memberCountReachedBeforeWaitReturnsImmediately() {
        ReentrantLock lock = new ReentrantLock();
        Condition membershipChanged = lock.newCondition();

        assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
                assertTrue(HadoopArrowFlightServer.awaitMemberCount(
                        () -> 3, lock, membershipChanged, 3, 100)));
    }

    /** Verifies spurious membership notifications do not satisfy the condition. */
    @Test
    void spuriousMembershipNotificationKeepsWaiting() throws Exception {
        ReentrantLock lock = new ReentrantLock();
        Condition membershipChanged = lock.newCondition();
        AtomicInteger members = new AtomicInteger(1);
        CountDownLatch countChecked = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> result = executor.submit(() ->
                    HadoopArrowFlightServer.awaitMemberCount(() -> {
                        countChecked.countDown();
                        return members.get();
                    }, lock, membershipChanged, 3, 500));
            assertTrue(countChecked.await(1, TimeUnit.SECONDS));

            lock.lock();
            try {
                membershipChanged.signalAll();
            } finally {
                lock.unlock();
            }
            assertThrows(TimeoutException.class,
                    () -> result.get(50, TimeUnit.MILLISECONDS));

            members.set(3);
            lock.lock();
            try {
                membershipChanged.signalAll();
            } finally {
                lock.unlock();
            }
            assertTrue(result.get(1, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    /** Verifies missing membership notifications end at configured timeout. */
    @Test
    void missingMembershipNotificationTimesOut() {
        ReentrantLock lock = new ReentrantLock();
        Condition membershipChanged = lock.newCondition();

        assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
                assertFalse(HadoopArrowFlightServer.awaitMemberCount(
                        () -> 1, lock, membershipChanged, 3, 25)));
    }
}
