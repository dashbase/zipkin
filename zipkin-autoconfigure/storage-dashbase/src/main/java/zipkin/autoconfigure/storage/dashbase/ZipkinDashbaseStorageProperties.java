package zipkin.autoconfigure.storage.dashbase;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.Serializable;

@ConfigurationProperties("zipkin.storage.dashbase")
public class ZipkinDashbaseStorageProperties implements Serializable { // for Spark jobs
  private static final long serialVersionUID = 0L;

  private String apiUrl;
  private String kafkaUrl;
  private String topic;
  private String tableName;

  public String getApiUrl() {
    return apiUrl;
  }

  public void setApiUrl(String apiUrl) {
    this.apiUrl = apiUrl;
  }

  public String getKafkaUrl() {
    return kafkaUrl;
  }

  public void setKafkaUrl(String kafkaUrl) {
    this.kafkaUrl = kafkaUrl;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getTopic() {
    return topic;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

}
