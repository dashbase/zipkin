package zipkin.autoconfigure.storage.dashbase;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import zipkin2.storage.StorageComponent;

@Configuration
@EnableConfigurationProperties(ZipkinDashbaseStorageProperties.class)
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "dashbase")
@ConditionalOnMissingBean(StorageComponent.class)
public class ZipkinDashbaseStorageAutoConfiguration {
  @Autowired(required = false)
  ZipkinDashbaseStorageProperties dashbase;
}
