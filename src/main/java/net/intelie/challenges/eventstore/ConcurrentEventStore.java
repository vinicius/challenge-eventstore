package net.intelie.challenges.eventstore;

import net.intelie.challenges.eventstore.model.Event;
import net.intelie.challenges.eventstore.interfaces.EventIterator;
import net.intelie.challenges.eventstore.interfaces.EventStore;

import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;


/**
 * A concurrent event store implementation. It uses a thread safe ordered map for storing events. Most operations on this
 * map can be safely executed by multiple threads.
 *
 * Concurrent operations on the event map can be performed but iterators provide weakly consistent traversal, which
 * means that they are guaranteed to traverse elements as they existed on iterator's construction. Any modifications
 * on the events map are not guaranteed to be reflected on this traversal.
 * This is a choice for concurrency over consistency.
 */
public class ConcurrentEventStore implements EventStore {

    /**
     * Map of events sorted by its keys.
     * It uses the timestamp of the event as the key. This implies that our map can hold one event per timestamp.
     * This limitation can be dealt by using <Long, List<Event>> as the map entry, accompanied by a more
     * sophisticated iterator.
     */
    ConcurrentSkipListMap<Long, Event> events = new ConcurrentSkipListMap<>();

    /**
     * Insert a new event on the events map.
     * Two events with the same timestamp will be treated as the same event and the later to be inserted will overwrite
     * the former.
     *
     * @param event event to be inserted
     */
    @Override
    public void insert(Event event) {
        this.events.put(event.timestamp(), event);
    }

    /**
     * Remove all events of a given type from the events map.
     *
     * @param type type of the event(s) to be removed
     */
    @Override
    public void removeAll(String type) {
        for(Iterator<Event> iterator = this.events.values().iterator(); iterator.hasNext();) {
            Event entry = iterator.next();
            if(entry.type().equals(type)) {
                iterator.remove();
            }
        }
    }

    /**
     * Query the event store to retrieve an iterator for the subset of events that correspond to the events with
     * timestamp inside a given range and of a given type.
     *
     * @param type      The type we are querying for.
     * @param startTime Start timestamp (inclusive).
     * @param endTime   End timestamp (exclusive).
     *
     * @return an event iterator for the event subset
     */
    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        return new EventByTypeIterator(this.events.subMap(startTime, endTime).values().iterator(), type);
    }
}
