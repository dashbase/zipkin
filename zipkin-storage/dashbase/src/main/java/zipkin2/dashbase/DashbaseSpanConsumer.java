package zipkin2.dashbase;

import io.dashbase.avro.DashbaseEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.dashbase.kafka.KafkaSink;
import zipkin2.storage.SpanConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class DashbaseSpanConsumer implements SpanConsumer {
  private static final Logger logger = LoggerFactory.getLogger(DashbaseSpanConsumer.class);
  private final Function<List<Span>, List<DashbaseEvent>> spanConverter;
  private final KafkaSink kafkaSink;
  private final String topic;

  DashbaseSpanConsumer(Function<List<Span>, List<DashbaseEvent>> spanConverter, KafkaSink kafkaSink, String topic) {
    this.spanConverter = spanConverter;
    this.kafkaSink = kafkaSink;
    this.topic = topic;
  }

  @Override
  public Call<Void> accept(List<Span> list) {
    List<DashbaseEvent> eventList = spanConverter.apply(list);
    logger.info("Produce dashbas events:" + eventList.toString());

    List<byte[]> bytesList = new ArrayList<>();
    eventList.forEach(event -> {
      try {
        bytesList.add(event.toByteBuffer().array());
      } catch (IOException e) {
        e.printStackTrace();
      }
    });

    kafkaSink.sendBulk(topic, bytesList);

    return Call.create(null /* Void == null */);
  }
}
