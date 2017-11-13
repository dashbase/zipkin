package zipkin.autoconfigure.storage.dashbase;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin.internal.V2StorageComponent;
import zipkin.storage.StorageComponent;
import zipkin2.dashbase.DashbaseStorage;

@Configuration
@EnableConfigurationProperties(ZipkinDashbaseStorageProperties.class)
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "dashbase")
@ConditionalOnMissingBean(StorageComponent.class)
public class ZipkinDashbaseStorageAutoConfiguration {
  @Autowired(required = false)
  ZipkinDashbaseStorageProperties props;

  @Bean
  StorageComponent storage(
    @Value("${zipkin.storage.strict-trace-id:true}") boolean strictTraceId
  ) {
    return V2StorageComponent.create(DashbaseStorage.newBuilder()
      .strictTraceId(strictTraceId)
      .apiUrl(props.getApiUrl())
      .kafkaUrl(props.getKafkaUrl())
      .topic(props.getTopic())
      .tableName(props.getTableName())
      .ssl(props.isSsl())
      .keystoreLocation(props.getKeystoreLocation())
      .keystorePassword(props.getKeystorePassword())
      .maxResultsNum(props.getMaxResultsNum())
      .build());
  }

}
