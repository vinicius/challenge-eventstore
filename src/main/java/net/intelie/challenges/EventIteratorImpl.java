package net.intelie.challenges;

import java.util.Iterator;
import java.util.Map;

public class EventIteratorImpl implements EventIterator {

    private Iterator<Map.Entry<Long, Event>> iterator;
    private String type;
    private Map.Entry<Long, Event> current = null;

    public EventIteratorImpl(Iterator<Map.Entry<Long, Event>> iterator, String type) {
        this.iterator = iterator;
        this.type = type;
    }

    @Override
    public boolean moveNext() {
        Map.Entry<Long, Event> next;
        while(iterator.hasNext()) {
            next = iterator.next();
            if(next.getValue().type().equals(type)) {
                current = next;
                return true;
            }
        }
        return false;
    }

    @Override
    public Event current() {
        return current != null ? current.getValue() : null;
    }

    @Override
    public void remove() {
        iterator.remove();
    }

    @Override
    public void close() throws Exception {
        System.out.println("Closed Event Iterator");
    }
}
