package zipkin2.dashbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.dashbase.kafka.KafkaConfiguration;
import zipkin2.dashbase.kafka.KafkaSink;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;

public class DashbaseStorage extends StorageComponent {
  private static final Logger logger = LoggerFactory.getLogger(DashbaseStorage.class);

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder extends StorageComponent.Builder {
    boolean strictTraceId = true;
    String apiUrl;
    String kafkaUrl;
    String topic;
    String tableName;
    boolean ssl;
    String keystoreLocation;
    String keystorePassword;

    @Override
    public Builder strictTraceId(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
      return this;
    }

    public Builder apiUrl(String apiUrl) {
      this.apiUrl = apiUrl;
      return this;
    }

    public Builder kafkaUrl(String kafkaUrl) {
      this.kafkaUrl = kafkaUrl;
      return this;
    }

    public Builder topic(String topic) {
      this.topic = topic;
      return this;
    }

    public Builder tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder ssl(boolean ssl) {
      this.ssl = ssl;
      return this;
    }

    public Builder keystoreLocation(String keystoreLocation) {
      this.keystoreLocation = keystoreLocation;
      return this;
    }

    public Builder keystorePassword(String keystorePassword) {
      this.keystorePassword = keystorePassword;
      return this;
    }

    @Override
    public DashbaseStorage build() {
      return new DashbaseStorage(this);
    }
  }

  private final boolean strictTraceId;
  private final String apiUrl;
  private final String kafkaUrl;
  private final String topic;
  private final String tableName;
  boolean ssl;
  String keystoreLocation;
  String keystorePassword;
  private final KafkaSink kafkaSink;
  private final SpanConverter converter = new SpanConverter();

  DashbaseStorage(Builder builder) {
    strictTraceId = builder.strictTraceId;
    apiUrl = builder.apiUrl;
    kafkaUrl = builder.kafkaUrl;
    topic = builder.topic;
    tableName = builder.tableName;
    ssl = builder.ssl;
    keystoreLocation = builder.keystoreLocation;
    keystorePassword = builder.keystorePassword;

    KafkaConfiguration configuration = new KafkaConfiguration();
    configuration.hosts = kafkaUrl;
    if (ssl) {
      configuration.kafkaProps.put("security.protocol", "SSL");
      configuration.kafkaProps.put("ssl.keystore.location", keystoreLocation);
      configuration.kafkaProps.put("ssl.truststore.location", keystoreLocation);
      configuration.kafkaProps.put("ssl.keystore.password", keystorePassword);
      configuration.kafkaProps.put("ssl.truststore.password", keystorePassword);
      configuration.kafkaProps.put("ssl.key.password", keystorePassword);
    }
    kafkaSink = new KafkaSink(configuration);
    logger.debug("Start dashbase backend! api:{} kafka:{}", apiUrl, kafkaUrl);
  }

  @Override
  public SpanStore spanStore() {
    return new DashbaseSpanStore();
  }

  @Override
  public SpanConsumer spanConsumer() {
    return new DashbaseSpanConsumer(converter, kafkaSink, topic);
  }
}
