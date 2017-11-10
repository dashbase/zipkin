package zipkin2.dashbase;

import io.dashbase.avro.DashbaseEvent;
import io.dashbase.avro.DashbaseEventBuilder;
import zipkin2.Endpoint;
import zipkin2.Span;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class SpanToDashbaseEventConverter implements Function<Span, DashbaseEvent> {

  @Override
  public DashbaseEvent apply(Span span) {
    long timeInMillis = TimeUnit.MICROSECONDS.toMillis(span.timestamp());
    DashbaseEventBuilder builder = new DashbaseEventBuilder();

    builder.withTimeInMillis(timeInMillis)
      .addNumber("zipkin.ts",span.timestamp())
      .addNumber("duration", TimeUnit.MICROSECONDS.toMillis(span.duration()))
      .addMeta("name", span.name())
      .addId("id", span.id())
      .addId("traceId", span.traceId())
      .addMeta("kind", span.kind().toString());

    if (span.parentId() != null)
      builder.addId("parentId", span.parentId());
    if (span.localServiceName() != null)
      builder.addMeta("localServiceName", span.localServiceName());
    if (span.remoteServiceName() != null)
      builder.addMeta("remoteServiceName", span.remoteServiceName());

    addEndpoint(span.localEndpoint(), "localEndpoint", builder);
    addEndpoint(span.remoteEndpoint(), "remoteEndpoint", builder);

    span.tags().forEach((k, v) -> builder.addMeta("tags." + k, v));

    span.annotations().forEach(annotation -> builder.addNumber("annotations." + annotation.value(), annotation.timestamp()));

    return builder.build();
  }

  private static void addEndpoint(Endpoint endpoint, String endpointName, DashbaseEventBuilder builder) {
    if (endpoint != null) {
      if (endpoint.ipv4() != null)
        builder.addMeta(endpointName + ".ipv4", endpoint.ipv4());
      if (endpoint.ipv6() != null)
        builder.addMeta(endpointName + ".ipv6", endpoint.ipv6());
      if (endpoint.port() != null)
        builder.addMeta(endpointName + ".port", endpoint.port().toString());
      if (endpoint.serviceName() != null)
        builder.addMeta(endpointName + ".serviceName", endpoint.serviceName());
    }
  }

}
