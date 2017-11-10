package zipkin2.dashbase.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class KafkaSink {
  private static Logger logger = LoggerFactory.getLogger(KafkaSink.class);

  private final KafkaProducer<byte[], byte[]> kafkaProducer;
  private Callback callback;

  public KafkaSink(KafkaConfiguration kafkaConfig) {
    String hosts = kafkaConfig.hosts;
    Properties props = new Properties();

    if (kafkaConfig.kafkaProps != null) {
      props.putAll(kafkaConfig.kafkaProps);
    }

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      hosts);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaConfig.clientId);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      ByteArraySerializer.class.getName());
    kafkaProducer = new KafkaProducer<>(props);
    this.callback = (meta, e) -> {
      if (e != null)
        logger.warn("Exception when sending data to kafka", e);
    };
  }

  public void send(String topic, byte[] bytes) {
    if (bytes == null) {
      return;
    }
    logger.debug("sending " + bytes.length + " bytes to topic: " + topic);
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, bytes);
    kafkaProducer.send(record, callback);
  }

  public void sendBulk(String topic, List<byte[]> bytesList) {
    if (bytesList == null || bytesList.isEmpty()) {
      return;
    }
    logger.debug("sending " + bytesList.size() + " records to topic: " + topic);
    for (byte[] bytes : bytesList) {
      ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, bytes);
      kafkaProducer.send(record, callback);
    }
  }
}
