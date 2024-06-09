package testing;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Instant;

public class EventGenerator implements SourceFunction<RentlyEvents> {

    private volatile boolean running = true;
    private int limitingTimestamp = Integer.MAX_VALUE;
    @Override
    public void run(SourceContext<RentlyEvents> sourceContext) throws Exception {
        long id = 1;

        while (running) {
            RentlyEvents event = new RentlyEvents(id);

            // don't emit events that exceed the specified limit
            if (event.activity_time >= limitingTimestamp) {
                break;
            }

            ++id;
            sourceContext.collect(event);

            // match our event production rate to that of the TaxiRideGenerator
            Thread.sleep(200);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
