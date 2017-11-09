package zipkin2.dashbase;

import io.dashbase.avro.DashbaseEvent;
import io.dashbase.avro.DashbaseEventBuilder;
import zipkin2.Span;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class SpanToDashbaseEventConverter implements Function<Span, DashbaseEvent> {

    @Override
    public DashbaseEvent apply(Span span) {
        long timeInMillis = TimeUnit.MICROSECONDS.toMillis(span.timestamp().longValue());
        DashbaseEventBuilder builder = new DashbaseEventBuilder();

        builder.withTimeInMillis(timeInMillis)
                .addNumber("duration", TimeUnit.MICROSECONDS.toMillis(span.duration().longValue()))
                .addMeta("name", span.name())
                .addId("id", span.id())
                .addId("parentId", span.parentId())
                .addId("traceId", span.traceId())
                .addMeta("localServiceName", span.localServiceName())
                .addMeta("remoteServiceName", span.remoteServiceName());

        // TODO:
        // * add local and remote endpoints
        // * add tags
        // * add annotations

        return builder.build();
    }
}
