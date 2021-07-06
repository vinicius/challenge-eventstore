package net.intelie.challenges.eventstore;

import net.intelie.challenges.eventstore.model.Event;
import net.intelie.challenges.eventstore.interfaces.EventIterator;
import net.intelie.challenges.eventstore.interfaces.EventStore;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for the event store map operations.
 * Operations on the map are performed through an iterator (except inserts).
 */
public class EventStoreTests {

    /**
     * Tests the creation of a new event.
     */
    @Test
    public void testEventCreation() {
        Event event = new Event("some_type", 123L);
        assertEquals(123L, event.timestamp());
        assertEquals("some_type", event.type());
    }

    /**
     * Tests the insert of a new event by querying it on the event store.
     */
    @Test
    public void testInsertAndQueryEvent() {
        Event event = new Event("A", 360000L);
        EventStore eventStore = new ConcurrentEventStore();
        eventStore.insert(event);
        EventIterator eventIterator = eventStore.query("A", event.timestamp(), event.timestamp() + 1);
        eventIterator.moveNext();
        assertEquals(event.timestamp(), eventIterator.current().timestamp());
    }

    /**
     * Insert multiple events and query the event map for them using different time ranges.
     */
    @Test
    public void testQueryEvents() {
        Event event1 = new Event("A", 360000L);
        Event event2 = new Event("A", 360003L);
        Event event3 = new Event("A", 340423L);

        EventStore eventStore = new ConcurrentEventStore();
        eventStore.insert(event1);
        eventStore.insert(event2);
        eventStore.insert(event3);

        EventIterator eventIterator = eventStore.query("A", 0L, 400000L);
        assertTrue(eventIterator.moveNext());
        assertEquals(event3.timestamp(), eventIterator.current().timestamp());
        assertTrue(eventIterator.moveNext());
        assertEquals(event1.timestamp(), eventIterator.current().timestamp());
        assertTrue(eventIterator.moveNext());
        assertEquals(event2.timestamp(), eventIterator.current().timestamp());

        eventIterator = eventStore.query("A", 350000L, 400000L);
        assertTrue(eventIterator.moveNext());
        assertEquals(event1.timestamp(), eventIterator.current().timestamp());
        assertTrue(eventIterator.moveNext());
        assertEquals(event2.timestamp(), eventIterator.current().timestamp());

        eventIterator = eventStore.query("A", 350000L, 360003L);
        assertTrue(eventIterator.moveNext());
        assertEquals(event1.timestamp(), eventIterator.current().timestamp());

        eventIterator = eventStore.query("A", 340423L, 360003L);
        assertTrue(eventIterator.moveNext());
        assertEquals(event3.timestamp(), eventIterator.current().timestamp());
        assertTrue(eventIterator.moveNext());
        assertEquals(event1.timestamp(), eventIterator.current().timestamp());
    }

    /**
     * Testing event overriding when they have same timestamp.
     * The last to be inserted overrides the previous one.
     */
    @Test
    public void testUnsupportedEventsSameTimestamp() {
        Event event1 = new Event("A", 360000L);
        Event event2 = new Event("B", 360000L);

        EventStore eventStore = new ConcurrentEventStore();
        eventStore.insert(event1);
        eventStore.insert(event2);

        EventIterator eventIterator = eventStore.query("A", 0L, 400000L);
        assertFalse(eventIterator.moveNext());

        eventIterator = eventStore.query("B", 0L, 400000L);
        assertTrue(eventIterator.moveNext());
        assertEquals(event1.timestamp(), eventIterator.current().timestamp());
        assertFalse(eventIterator.moveNext());
    }

    /**
     * Test the exception throw by the iterator when move next was false and current event is called.
     */
    @Test
    public void testGetCurrentWithException() {
        Event event1 = new Event("A", 360000L);
        Event event2 = new Event("C", 360003L);
        Event event3 = new Event("A", 340423L);

        EventStore eventStore = new ConcurrentEventStore();
        eventStore.insert(event1);
        eventStore.insert(event2);
        eventStore.insert(event3);

        EventIterator eventIterator = eventStore.query("B", 340423L, 360003L);
        assertFalse(eventIterator.moveNext());
        try {
            eventIterator.current();
            fail("Should have thrown IllegalStateException");
        } catch (IllegalStateException ise) {
            // expected behavior
        } catch (Exception e) {
            fail("Should have thrown IllegalStateException, not " + e.getClass().getName());
        }
    }

    /**
     * Testing querying events by type.
     */
    @Test
    public void testQueryEventsByType() {
        Event event1 = new Event("A", 360000L);
        Event event2 = new Event("B", 360003L);
        Event event3 = new Event("C", 340423L);
        Event event4 = new Event("A", 350423L);
        Event event5 = new Event("C", 40423L);

        EventStore eventStore = new ConcurrentEventStore();
        eventStore.insert(event1);
        eventStore.insert(event2);
        eventStore.insert(event3);
        eventStore.insert(event4);
        eventStore.insert(event5);

        EventIterator eventIterator = eventStore.query("A", 0L, 400000L);
        eventIterator.moveNext();
        assertEquals(event4.timestamp(), eventIterator.current().timestamp());
        eventIterator.moveNext();
        assertEquals(event1.timestamp(), eventIterator.current().timestamp());

        eventIterator = eventStore.query("B", 0L, 400000L);
        eventIterator.moveNext();
        assertEquals(event2.timestamp(), eventIterator.current().timestamp());

        eventIterator = eventStore.query("C", 0L, 400000L);
        eventIterator.moveNext();
        assertEquals(event5.timestamp(), eventIterator.current().timestamp());
        eventIterator.moveNext();
        assertEquals(event3.timestamp(), eventIterator.current().timestamp());
    }

    /**
     * Test the removal of events from the event store by a given type
     */
    @Test
    public void testRemoveAllByType() {
        Event event1 = new Event("A", 360000L);
        Event event2 = new Event("B", 360003L);
        Event event3 = new Event("C", 340423L);
        Event event4 = new Event("A", 350423L);
        Event event5 = new Event("C", 40423L);

        EventStore eventStore = new ConcurrentEventStore();
        eventStore.insert(event1);
        eventStore.insert(event2);
        eventStore.insert(event3);
        eventStore.insert(event4);
        eventStore.insert(event5);
        eventStore.removeAll("A");

        EventIterator eventIterator = eventStore.query("A", 0L, 400000L);
        assertFalse(eventIterator.moveNext());
        try {
            eventIterator.current();
            fail("Should have thrown IllegalStateException");
        } catch (IllegalStateException ise) {
            // expected behavior
        }

        eventIterator = eventStore.query("C", 0L, 400000L);
        eventIterator.moveNext();
        assertEquals(event5.timestamp(), eventIterator.current().timestamp());
        eventIterator.moveNext();
        assertEquals(event3.timestamp(), eventIterator.current().timestamp());

        eventIterator = eventStore.query("B", 0L, 400000L);
        eventIterator.moveNext();
        assertEquals(event2.timestamp(), eventIterator.current().timestamp());
    }

    /**
     * Test an empty store (and the exception thrown in case of current operation).
     */
    @Test
    public void testEmptyStore() {
        EventStore emptyStore = new ConcurrentEventStore();
        EventIterator eventIterator = emptyStore.query("any", 0L, 10000000L);
        assertFalse(eventIterator.moveNext());
        try {
            eventIterator.current();
            fail("Should have thrown IllegalStateException");
        } catch (IllegalStateException ise) {
            // expected behavior
        }
    }

    /**
     * Test if current operation is null before calling move next operation for the first time.
     */
    @Test
    public void testIteratorCurrentNullBeforeMove() {
        Event event1 = new Event("A", 360000L);

        EventStore emptyStore = new ConcurrentEventStore();
        emptyStore.insert(event1);

        EventIterator eventIterator = emptyStore.query("any", 0L, 10000000L);
        try {
            eventIterator.current();
            fail("Should have thrown IllegalStateException");
        } catch (IllegalStateException ise) {
            // expected behavior
        }
    }

}