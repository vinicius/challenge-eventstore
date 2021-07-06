package net.intelie.challenges.eventstore.model;

/**
 * Event model class with type and timestamp.
 */
public class Event {

    /**
     * Event type.
     */
    private final String type;

    /**
     * Event timestamp in milliseconds.
     */
    private final long timestamp;

    /**
     * Event constructor.
     *
     * @param type
     * @param timestamp
     */
    public Event(String type, long timestamp) {
        this.type = type;
        this.timestamp = timestamp;
    }

    /**
     * Event type getter.
     *
     * @return type of event
     */
    public String type() {
        return type;
    }

    /**
     * Event timestamp getter.
     *
     * @return timestamp of event
     */
    public long timestamp() {
        return timestamp;
    }
}
