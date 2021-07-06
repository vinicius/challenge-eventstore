package net.intelie.challenges.eventstore;

import net.intelie.challenges.eventstore.model.Event;
import net.intelie.challenges.eventstore.interfaces.EventIterator;
import net.intelie.challenges.eventstore.interfaces.EventStore;
import org.junit.Assert;
import org.junit.Test;

import java.time.OffsetDateTime;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Tests the event store in concurrent scenarios.
 * Although we fixed a wait time for the threads to finish on each test, it may happen that in some executions
 * it take longer than that. In that case we recommend increasing the wait time.
 *
 * We use a random generator based timestamps of the events. Although rare, identical timestamps might happen in some
 * test execution.
 */
public class EventStoreConcurrenceTests {

    private int waitTime = 15;

    /**
     * Test concurrent insertions.
     *
     * @throws InterruptedException
     */
    @Test
    public void testConcurrentInsertions() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(6);
        EventStore eventStore = new ConcurrentEventStore();
        int numberOfThreads = 3;

        Runnable producer = () -> IntStream
                .rangeClosed(1, 100)
                .forEach(index -> eventStore.insert(new Event("some type", OffsetDateTime.now().minusSeconds(new Random().nextInt(10000000)).toInstant().toEpochMilli())));

        for (int i = 0; i < numberOfThreads; i++) {
            executorService.execute(producer);
        }
        System.out.println("Waiting all events to be inserted...");
        if (!executorService.awaitTermination(waitTime, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
        }
        EventIterator it = eventStore.query("some type", 0L, 2625293189598L);
        int count = 0;
        while(it.moveNext()) {
            count++;
        }
        System.out.println("Done insertion of " + count);
        Assert.assertEquals(count, 300);
    }

    /**
     * Test concurrent insertions and queries.
     *
     * @throws InterruptedException
     */
    @Test
    public void testConcurrentInsertionsAndQueries() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        EventStore eventStore = new ConcurrentEventStore();
        int numberOfThreads = 3;

        Runnable insertProducer = () -> IntStream
                .rangeClosed(1, 100)
                .forEach(index -> eventStore.insert(new Event("some type", OffsetDateTime.now().minusSeconds(new Random().nextInt(10000000)).toInstant().toEpochMilli())));

        Runnable queryProducer = () -> IntStream
                .rangeClosed(1, 100)
                .forEach(index -> eventStore.query("some type", 0L, OffsetDateTime.now().toInstant().toEpochMilli()));


        for (int i = 0; i < numberOfThreads - 2; i++) {
            executorService.execute(insertProducer);
        }

        for (int i = numberOfThreads - 2; i < numberOfThreads; i++) {
            executorService.execute(queryProducer);
        }

        System.out.println("Waiting all events to be inserted...");
        if (!executorService.awaitTermination(waitTime, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
        }
        EventIterator it = eventStore.query("some type", 0L, 2625293189598L);
        int count = 0;
        while(it.moveNext()) {
            count++;
        }
        System.out.println("Done insertion of " + count);
        Assert.assertEquals(count, 100);
    }

    /**
     * Tests concurrent insertions followed by event removals.
     * After the removals the remaining number of events is verified.
     *
     * @throws InterruptedException
     */
    @Test
    public void testConcurrentInsertionsAndDeletes() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        EventStore eventStore = new ConcurrentEventStore();
        int numberOfThreads = 3;

        Runnable insertProducer = () -> IntStream
                .rangeClosed(1, 100)
                .forEach(index -> eventStore.insert(new Event("some type", OffsetDateTime.now().minusSeconds(new Random().nextInt(10000000)).toInstant().toEpochMilli())));


        for (int i = 0; i < numberOfThreads - 1; i++) {
            executorService.execute(insertProducer);
        }

        System.out.println("Waiting all events to be performed...");
        if (!executorService.awaitTermination(waitTime, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
        }

        EventIterator it = eventStore.query("some type", 0L, 2625293189598L);
        int count = 0;
        while(it.moveNext()) {
            count++;
        }
        System.out.println("Done insertion of " + count);

        EventIterator ei = eventStore.query("some type", 0L, 2625293189598L);
        int toRemove = 100;
        while(ei.moveNext() && toRemove > 0) {
            ei.remove();
            toRemove--;
        }
        it = eventStore.query("some type", 0L, 2625293189598L);
        count = 0;
        while(it.moveNext()) {
            count++;
        }
        System.out.println("Total after deletions:" + count);
        Assert.assertEquals(count, 100);
    }

    /**
     * Tests concurrent insertions and removals. The remaining can be any number between 0 and the total inserted.
     *
     * @throws InterruptedException
     */
    @Test
    public void testConcurrentRandomInsertionsAndDeletes() throws InterruptedException {
        int numberOfThreads = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
        EventStore eventStore = new ConcurrentEventStore();

        Runnable insertProducer = () -> IntStream
                .rangeClosed(1, 1000)
                .forEach(index -> eventStore.insert(new Event("some type", OffsetDateTime.now().minusSeconds(new Random().nextInt(10000000)).toInstant().toEpochMilli())));

        Runnable deleteProducer = () -> IntStream
                .rangeClosed(1, 1000)
                .forEach(index -> {
                    EventIterator eit = eventStore.query("some type", 0L, 2625293189598L);
                    if(eit.moveNext()) {
                        eit.remove();
                    }
                });

        for (int i = 0; i < numberOfThreads; i++) {
            executorService.execute(insertProducer);
            executorService.execute(deleteProducer);
        }



        System.out.println("Waiting all events to be performed...");
        if (!executorService.awaitTermination(waitTime, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
        }

        EventIterator it = eventStore.query("some type", 0L, 2625293189598L);
        int count = 0;
        while(it.moveNext()) {
            count++;
        }
        Assert.assertTrue(count > 0);
        Assert.assertTrue(count < 100000);
        System.out.println("Remains " + count + " after parallel insertions and deletions ");
    }
}
