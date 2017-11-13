package zipkin2.dashbase;

import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rapid.api.RapidRequest;
import zipkin2.elasticsearch.internal.client.HttpCall;

public class DashbaseHttpService {
  private final static Logger logger = LoggerFactory.getLogger(DashbaseHttpService.class);
  private static OkHttpClient client;
  private static final String V1_PATH_QUERY = "/v1/query";
  private String queryUrl;
  private HttpCall.Factory http;

  public DashbaseHttpService(String apiUrl) {
    this.queryUrl = apiUrl + V1_PATH_QUERY;
    // todo host verification
    client = (new OkHttpClient.Builder()).hostnameVerifier((hostname, session) -> true).build();
    http = new HttpCall.Factory(client, HttpUrl.parse(queryUrl));
  }

  public <V> HttpCall<V> query(RapidRequest rapidRequest, RapidResConverter<V> bodyConverter) {
    MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    RequestBody body = RequestBody.create(JSON, rapidRequest.toString());
    Request request = new Request.Builder()
      .url(queryUrl)
      .post(body)
      .build();
    logger.debug("Request:" + rapidRequest.toString());
    return http.newCall(request, bodyConverter);
  }
}
