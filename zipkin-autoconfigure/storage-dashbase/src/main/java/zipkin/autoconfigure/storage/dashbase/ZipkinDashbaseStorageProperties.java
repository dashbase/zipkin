package zipkin.autoconfigure.storage.dashbase;

import java.io.Serializable;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("zipkin.storage.dashbase")
public class ZipkinDashbaseStorageProperties implements Serializable { // for Spark jobs
  private static final long serialVersionUID = 0L;

  private String host = "localhost";
  private int port = 7888;

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

}
