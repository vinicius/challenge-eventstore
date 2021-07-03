package net.intelie.challenges;

import org.junit.Test;

import static org.junit.Assert.*;

public class EventStoreTest {

    @Test
    public void testEventCreation() throws Exception {
        Event event = new Event("some_type", 123L);
        assertEquals(123L, event.timestamp());
        assertEquals("some_type", event.type());
    }

    @Test
    public void testInsertAndQueryEvent() throws Exception {
        Event event = new Event("A", 360000L);
        EventStore eventStore = new ConcurrentEventStore();
        eventStore.insert(event);
        EventIterator eventIterator = eventStore.query("A", event.timestamp(), event.timestamp() + 1);
        eventIterator.moveNext();
        assertEquals(event.timestamp(), eventIterator.current().timestamp());
    }

    @Test
    public void testQueryEvents() throws Exception {
        Event event1 = new Event("A", 360000L);
        Event event2 = new Event("A", 360003L);
        Event event3 = new Event("A", 340423L);

        EventStore eventStore = new ConcurrentEventStore();
        eventStore.insert(event1);
        eventStore.insert(event2);
        eventStore.insert(event3);

        EventIterator eventIterator = eventStore.query("A", 0L, 400000L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(event3.timestamp(), eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(event1.timestamp(), eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(event2.timestamp(), eventIterator.current().timestamp());

        eventIterator = eventStore.query("A", 350000L, 400000L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(event1.timestamp(), eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(event2.timestamp(), eventIterator.current().timestamp());

        eventIterator = eventStore.query("A", 350000L, 360003L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(event1.timestamp(), eventIterator.current().timestamp());

        eventIterator = eventStore.query("A", 340423L, 360003L);
        assertEquals(true, eventIterator.moveNext());
        assertEquals(event3.timestamp(), eventIterator.current().timestamp());
        assertEquals(true, eventIterator.moveNext());
        assertEquals(event1.timestamp(), eventIterator.current().timestamp());
    }

    @Test
    public void testGetCurrentWithException() throws Exception {
        Event event1 = new Event("A", 360000L);
        Event event2 = new Event("C", 360003L);
        Event event3 = new Event("A", 340423L);

        EventStore eventStore = new ConcurrentEventStore();
        eventStore.insert(event1);
        eventStore.insert(event2);
        eventStore.insert(event3);

        EventIterator eventIterator = eventStore.query("B", 340423L, 360003L);
        assertEquals(false, eventIterator.moveNext());
        try {
            eventIterator.current();
            fail("Should have thrown IllegalStateException");
        } catch (IllegalStateException ise) {
            // expected behavior
        } catch (Exception e) {
            fail("Should have thrown IllegalStateException, not " + e.getClass().getName());
        }
    }

    @Test
    public void testQueryEventsByType() throws Exception {
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

    @Test
    public void testRemoveAllByType() throws Exception {
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
        assertEquals(false, eventIterator.moveNext());
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

    @Test
    public void testEmptyStore() throws Exception {
        EventStore emptyStore = new ConcurrentEventStore();
        EventIterator eventIterator = emptyStore.query("any", 0L, 10000000L);
        assertEquals(false, eventIterator.moveNext());
        try {
            eventIterator.current();
            fail("Should have thrown IllegalStateException");
        } catch (IllegalStateException ise) {
            // expected behavior
        }
    }

    @Test
    public void testIteratorCurrentNullBeforeMove() throws Exception {
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