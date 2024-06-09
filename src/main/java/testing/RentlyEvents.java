package testing;

import common.Events;
import common.User;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

public class RentlyEvents implements Serializable {
    public long hub_id;
    public long activity_time;
    public Events event_type;
    public User user_type;
    public RentlyEvents(){
        this.hub_id = 0;
    }
    public RentlyEvents(long hub_id){
        this.hub_id = hub_id;
        DataGenerator generator = new DataGenerator(hub_id);
        this.activity_time = generator.activity_time();
        this.event_type = generator.event_type();
        this.user_type = generator.user_type();
    }

    public RentlyEvents(long hub_id, long activity_time, Events event_type, User user_type) {
        this.hub_id = hub_id;
        this.activity_time = activity_time;
        this.event_type = event_type;
        this.user_type = user_type;
    }

    @Override
    public String toString() {
        return "RentlyEvents{" +
                "hub_id=" + hub_id +
                ", activity_time=" + activity_time +
                ", event_type=" + event_type +
                ", user_type=" + user_type +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RentlyEvents)) return false;
        RentlyEvents that = (RentlyEvents) o;
        return hub_id == that.hub_id && Objects.equals(activity_time, that.activity_time) && event_type == that.event_type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(hub_id, activity_time, event_type, user_type);
    }

}
