package zipkin2.dashbase;

import io.dashbase.avro.DashbaseEvent;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.SpanConsumer;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class DashbaseSpanConsumer implements SpanConsumer {

    private final Function<List<Span>, List<DashbaseEvent>> spanConverter;
    private final Consumer<DashbaseEvent> consumer;

    DashbaseSpanConsumer(Function<List<Span>, List<DashbaseEvent>> spanConverter, Consumer<DashbaseEvent> consumer) {
        this.spanConverter = spanConverter;
        this.consumer = consumer;

    }

    @Override
    public Call<Void> accept(List<Span> list) {
        /*
        Consumer<DashbaseEvent> consumer = new Consumer<DashbaseEvent>() {

            @Override
            public void accept(DashbaseEvent dashbaseEvent) {
                System.out.println(dashbaseEvent.getTimeInMillis());
            }
        };
*/
        List<DashbaseEvent> eventList = spanConverter.apply(list);


        eventList.stream().forEach(consumer);
        return Call.create(null /* Void == null */);
    }
}
