package net.intelie.challenges;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;


/**
 * A concurrent event store implementation. It uses a thread safe ordered map for storing events.
 * Concurrent operations on the event map can be performed but iterators are weakly consistent.
 */
public class ConcurrentEventStore implements EventStore {

    /**
     * Map of events, keyed by their timestamp
     */
    ConcurrentSkipListMap<Long, Event> events = new ConcurrentSkipListMap<>();

    @Override
    public void insert(Event event) {
        this.events.put(event.timestamp(), event);
    }

    @Override
    public void removeAll(String type) {
        for(Iterator<Map.Entry<Long, Event>> iterator = this.events.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<Long, Event> entry = iterator.next();
            if(entry.getValue().type().equals(type)) {
                iterator.remove();
            }
        }
//        Set<Map.Entry<Long, Event>> eventsToRemove = this.events.entrySet();
//        eventsToRemove.removeIf(entry -> !entry.getValue().equals(type));
//        for(Iterator<Map.Entry<Long, Event>> iterator = eventsToRemove.iterator(); iterator.hasNext();) {
//            Map.Entry<Long, Event> entry = iterator.next();
//            iterator.remove();
//        }
    }

    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        return new EventByTypeIterator(this.events.subMap(startTime, endTime).values().stream().iterator(), type);
    }
}
