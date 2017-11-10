package zipkin2.dashbase;

import io.dashbase.avro.DashbaseEvent;
import zipkin2.Span;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public class SpanConverter implements Function<List<Span>, List<DashbaseEvent>> {
  private SpanToDashbaseEventConverter converter = new SpanToDashbaseEventConverter();

  @Override
  public List<DashbaseEvent> apply(List<Span> spans) {
    List<DashbaseEvent> eventList = new ArrayList<>();
    spans.forEach(span -> eventList.add(converter.apply(span)));
    return eventList;
  }
}
