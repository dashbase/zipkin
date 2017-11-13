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
  private boolean ssl;
  private String keystoreLocation;
  private String keystorePassword;
  private int maxResultsNum;

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

  public boolean isSsl() {
    return ssl;
  }

  public void setSsl(boolean ssl) {
    this.ssl = ssl;
  }

  public String getKeystoreLocation() {
    return keystoreLocation;
  }

  public void setKeystoreLocation(String keystoreLocation) {
    this.keystoreLocation = keystoreLocation;
  }

  public String getKeystorePassword() {
    return keystorePassword;
  }

  public void setKeystorePassword(String keystorePassword) {
    this.keystorePassword = keystorePassword;
  }

  public int getMaxResultsNum() {
    return maxResultsNum;
  }

  public void setMaxResultsNum(int maxResultsNum) {
    this.maxResultsNum = maxResultsNum;
  }
}
