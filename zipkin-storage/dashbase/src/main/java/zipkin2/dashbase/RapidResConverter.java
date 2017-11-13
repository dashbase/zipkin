package zipkin2.dashbase;

import com.fasterxml.jackson.databind.ObjectMapper;
import okio.BufferedSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rapid.api.RapidResponse;
import zipkin2.elasticsearch.internal.client.HttpCall;

import java.io.IOException;

public abstract class RapidResConverter<V> implements HttpCall.BodyConverter<V> {
  private final static ObjectMapper mapper = new ObjectMapper();
  private final static Logger logger = LoggerFactory.getLogger(RapidResConverter.class);

  @Override
  public V convert(BufferedSource content) throws IOException {
    RapidResponse res;
    try {
      res = mapper.readValue(content.readUtf8(), RapidResponse.class);
      logger.debug("Get the response.");
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
    return convert(res);
  }

  public abstract V convert(RapidResponse res);
}

