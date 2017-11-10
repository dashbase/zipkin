package zipkin2.dashbase.kafka;

import com.google.common.collect.Maps;

import java.util.Map;

public class KafkaConfiguration {

  public static final String DEFAULT_KAFKA_CLIENT_ID = "DashbaseKafkaSink";

  public String hosts;

  public String clientId = DEFAULT_KAFKA_CLIENT_ID;

  public Map<String, String> kafkaProps = Maps.newHashMap();
}
