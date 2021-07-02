package net.intelie.challenges;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class EventStoreImpl implements EventStore {

    ConcurrentSkipListMap<Long, Event> events = new ConcurrentSkipListMap();

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
        Set<Map.Entry<Long, Event>> eventsToRemove = this.events.entrySet();
        eventsToRemove.removeIf(entry -> !entry.getValue().equals(type));
        for(Iterator<Map.Entry<Long, Event>> iterator = eventsToRemove.iterator(); iterator.hasNext();) {
            Map.Entry<Long, Event> entry = iterator.next();
            iterator.remove();
        }
    }

    @Override
    public EventIterator query(String type, long startTime, long endTime) {
        return new EventIteratorImpl(this.events.subMap(startTime, endTime).entrySet().stream().iterator(), type);
    }
}
