package net.intelie.challenges;

import java.util.Iterator;

/**
 * Iterator for {@link Event} collections.
 * It wraps a java util iterator to walk through events of a given type.
 */
public class EventByTypeIterator implements EventIterator {

    /** Event iterator **/
    private Iterator<Event> iterator;

    /** Event type of the iterator **/
    private String type;

    /** Current iterator event **/
    private Event current = null;

    /** Whether the iterator has reached the end or not **/
    private boolean endReached = false;

    /**
     * Public constructor of the event iterator
     * @param iterator java util iterator
     * @param type event type
     */
    public EventByTypeIterator(Iterator<Event> iterator, String type) {
        this.iterator = iterator;
        this.type = type;
    }

    /**
     * Move the iterator to the next event of the give type.
     * @return true if the move was possible, false if end was reached or there is not more events of the give type.
     */
    @Override
    public boolean moveNext() {
        Event next;
        while(iterator.hasNext()) {
            next = iterator.next();
            if(next.type().equals(type)) {
                current = next;
                return true;
            }
        }
        endReached = true;
        return false;
    }

    /**
     * Return the current event of the given type
     *
     * @return current event
     */
    @Override
    public Event current() {
        if(current == null || endReached) {
            throw new IllegalStateException();
        }
        return current;
    }

    /**
     * Removes the current event
     */
    @Override
    public void remove() {
        if(current == null || endReached) {
            throw new IllegalStateException();
        }
        iterator.remove();
    }

    @Override
    public void close() throws Exception {
        System.out.println("Closed Event Iterator");
    }
}
