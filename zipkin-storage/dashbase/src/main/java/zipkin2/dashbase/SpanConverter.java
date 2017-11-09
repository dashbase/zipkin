package zipkin2.dashbase;

import io.dashbase.avro.DashbaseEvent;
import io.dashbase.avro.DashbaseEventBuilder;
import zipkin2.Span;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public class SpanConverter implements Function<List<Span>, List<DashbaseEvent>> {

    @Override
    public List<DashbaseEvent> apply(List<Span> spans) {
        List<DashbaseEvent> eventList = new LinkedList<>();
        for (Span span : spans) {
            DashbaseEventBuilder builder = new DashbaseEventBuilder();
            builder.withOmitPayload(true).withTimeInMillis(span.timestamp());
            eventList.add(builder.build());
        }
        return eventList;
    }
}
