package zipkin2.dashbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rapid.api.RapidRequest;
import rapid.api.RapidResponse;
import rapid.api.TimeRangeFilter;
import rapid.api.query.*;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.elasticsearch.internal.client.HttpCall;
import zipkin2.internal.DependencyLinker;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;

import java.util.*;
import java.util.concurrent.TimeUnit;

final public class DashbaseSpanStore implements SpanStore {
  private final static Logger logger = LoggerFactory.getLogger(DashbaseSpanStore.class);
  private final static long EARLIEST_MS = 1456790400000L; // March 2016

  private DashbaseHttpService service;
  private boolean strictTraceId;
  private String tableName;
  private int maxResultsNum;

  public DashbaseSpanStore(String apiUrl, String tableName, int maxResultsNum, boolean strictTraceId) {
    service = new DashbaseHttpService(apiUrl);
    this.strictTraceId = strictTraceId;
    this.tableName = tableName;
    this.maxResultsNum = maxResultsNum;
  }

  @Override
  public Call<List<List<Span>>> getTraces(QueryRequest zipkinReq) {
    RapidRequest rapidRequest = getRapidRequest();
    long endMillis = TimeUnit.MILLISECONDS.toSeconds(zipkinReq.endTs());
    long beginMillis = TimeUnit.MILLISECONDS.toSeconds(
      Math.max(zipkinReq.endTs() - zipkinReq.lookback(), EARLIEST_MS));
    rapidRequest.timeRangeFilter = new TimeRangeFilter(beginMillis, endMillis);
    rapidRequest.fields.add("traceId");

    List<Query> queries = new ArrayList<>();

    // service name
    if (zipkinReq.serviceName() != null)
      queries.add(new EqualityQuery("localServiceName", zipkinReq.serviceName()));

    // span name
    if (zipkinReq.spanName() != null)
      queries.add(new EqualityQuery("name", zipkinReq.spanName()));

    // duration
    if (zipkinReq.maxDuration() != null) {
      RangeQuery query = new RangeQuery();
      query.col = "duration";
      query.max = String.valueOf(zipkinReq.maxDuration());
      query.min = String.valueOf(zipkinReq.minDuration());
      query.maxInclusive = true;
      query.minInclusive = true;
      queries.add(query);
    }

    // annotations and tags
    zipkinReq.annotationQuery().forEach((k, v) -> {
      if (!Objects.equals(v, "")) {
        /*
          When an input value is the empty string, include traces whose {@link Span#annotations()}
          include a value in this set, or where {@link Span#tags()} include a key is in this set.
         */
        List<Query> disjunctionQueries = new ArrayList<>();
        disjunctionQueries.add(new EqualityQuery("tags." + k, "", false));
        disjunctionQueries.add(new EqualityQuery("annotations." + k, "", false));
        queries.add(new Disjunction(disjunctionQueries));
      } else {
        /*
          not, include traces whose {@link Span#tags()} an entry in this map.
         */
        queries.add(new EqualityQuery("tags." + k, v));
      }
    });

    if (queries.size() > 0)
      rapidRequest.query = new Conjunction(queries);

    // get trace ids
    HttpCall<Set<String>> traceIdsCall = service.query(rapidRequest, new RapidResConverter<Set<String>>() {
      @Override
      public Set<String> convert(RapidResponse res) {
        Set<String> ids = new HashSet<>();
        res.hits.forEach(hit -> {
          try {
            ids.add(hit.getPayload().fields.get("traceId").get(0));
          } catch (Exception ignore) {
          }
        });
        return ids;
      }
    });

    RapidResConverter<List<List<Span>>> convertIdsToTracks = new RapidResConverter<List<List<Span>>>() {
      @Override
      public List<List<Span>> convert(RapidResponse res) {
        List<Span> spans = SpansConverter.convert(res);
        List<List<Span>> traces = groupByTraceId(spans, strictTraceId);
        // Due to tokenization of the trace ID, our matches are imprecise on Span.traceIdHigh
        traces.removeIf(next -> next.get(0).traceId().length() > 16 && !zipkinReq.test(next));
        traces.sort(Comparator.comparingLong(o -> -o.get(0).timestamp()));
        traces = traces.subList(0, Math.min(zipkinReq.limit(), traces.size()));
        return traces;
      }
    };

    return traceIdsCall.flatMap(input -> {
      if (input.isEmpty()) return Call.emptyList();
      RapidRequest getTraces = getRapidRequest();
      getTraces.timeRangeFilter = new TimeRangeFilter(beginMillis, endMillis);
      getTraces.query = new InclusionQuery("traceId", input);
      return service.query(getTraces, convertIdsToTracks);
    });
  }

  @Override
  public Call<List<Span>> getTrace(String traceId) {
    RapidRequest getTrace = getRapidRequest();
    getTrace.query = new EqualityQuery("traceId", traceId);
    return service.query(getTrace, SpansConverter);
  }

  @Override
  public Call<List<String>> getServiceNames() {
    RapidRequest getServiceNames = getRapidRequest();
    getServiceNames.fields.add("localServiceName");
    return service.query(getServiceNames, new RapidResConverter<List<String>>() {
      @Override
      public List<String> convert(RapidResponse res) {
        Set<String> names = new HashSet<>();
        res.hits.forEach(hit -> {
          try {
            String name = hit.getPayload().fields.get("localServiceName").get(0);
            if (!name.isEmpty())
              names.add(name);
          } catch (Exception ignored) {
          }
        });
        logger.info("Service names: " + names);
        return new ArrayList<>(names);
      }
    });
  }

  @Override
  public Call<List<String>> getSpanNames(String s) {
    RapidRequest getSpanNames = getRapidRequest();
    getSpanNames.fields.add("name");
    return service.query(getSpanNames, new RapidResConverter<List<String>>() {
      @Override
      public List<String> convert(RapidResponse res) {
        Set<String> names = new HashSet<>();
        res.hits.forEach(hit -> {
          try {
            String name = hit.getPayload().fields.get("name").get(0);
            if (!name.isEmpty())
              names.add(name);
          } catch (Exception ignored) {
          }
        });
        logger.info("Span names: " + names);
        return new ArrayList<>(names);
      }
    });
  }

  @Override
  public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    RapidRequest getDep = getRapidRequest();
    long beginMillis = Math.max(endTs - lookback, EARLIEST_MS);
    getDep.timeRangeFilter = new TimeRangeFilter(
      TimeUnit.MILLISECONDS.toSeconds(beginMillis),
      TimeUnit.MILLISECONDS.toSeconds(endTs));

    return service.query(getDep, new RapidResConverter<List<DependencyLink>>() {
      @Override
      public List<DependencyLink> convert(RapidResponse res) {
        List<Span> spans = SpansConverter.convert(res);
        List<List<Span>> traces = groupByTraceId(spans, strictTraceId);
        traces.removeIf(next -> next.get(0).traceId().length() > 16);
        DependencyLinker linksBuilder = new DependencyLinker();
        for (Collection<Span> trace : traces) {
          // use a hash set to dedupe any redundantly accepted spans
          linksBuilder.putTrace(new HashSet<>(trace).iterator());
        }
        return linksBuilder.link();
      }
    });
  }

  // convert rapid response to traces.
  private RapidResConverter<List<Span>> SpansConverter = new RapidResConverter<List<Span>>() {
    @Override
    public List<Span> convert(RapidResponse res) {
      List<Span> spans = new ArrayList<>();
      res.hits.forEach(hit -> {
        try {
          Map<String, List<String>> fields = hit.getPayload().fields;
          Span.Builder spanBuilder = Span.newBuilder();
          spanBuilder.id(fields.get("id").get(0));
          spanBuilder.timestamp(Double.valueOf(fields.get("zipkin.ts").get(0)).longValue());
          spanBuilder.duration(TimeUnit.MILLISECONDS.toMicros(
            Double.valueOf(fields.get("duration").get(0)).longValue()
          ));
          spanBuilder.traceId(fields.get("traceId").get(0));
          spanBuilder.name(fields.get("name").get(0));
          spanBuilder.kind(Span.Kind.valueOf(fields.get("kind").get(0)));

          final Endpoint.Builder localBuilder = Endpoint.newBuilder(), remoteBuilder = Endpoint.newBuilder();
          boolean hasLocal = false, hasRemote = false;

          if (fields.containsKey("parentId"))
            spanBuilder.parentId(fields.get("parentId").get(0));

          // localEndpoint
          if (fields.containsKey("localEndpoint.ipv4")) {
            localBuilder.ip(fields.get("localEndpoint.ipv4").get(0));
            hasLocal = true;
          }
          if (fields.containsKey("localEndpoint.ipv6")) {
            localBuilder.ip(fields.get("localEndpoint.ipv6").get(0));
            hasLocal = true;
          }
          if (fields.containsKey("localEndpoint.port")) {
            localBuilder.port(Integer.valueOf(fields.get("localEndpoint.port").get(0)));
            hasLocal = true;
          }
          if (fields.containsKey("localEndpoint.serviceName")) {
            localBuilder.serviceName(fields.get("localEndpoint.serviceName").get(0));
            hasLocal = true;
          }

          // remoteEndpoint
          if (fields.containsKey("remoteEndpoint.ipv4")) {
            remoteBuilder.ip(fields.get("remoteEndpoint.ipv4").get(0));
            hasRemote = true;
          }
          if (fields.containsKey("remoteEndpoint.ipv6")) {
            remoteBuilder.ip(fields.get("remoteEndpoint.ipv6").get(0));
            hasRemote = true;
          }
          if (fields.containsKey("remoteEndpoint.port")) {
            remoteBuilder.port(Integer.valueOf(fields.get("remoteEndpoint.port").get(0)));
            hasRemote = true;
          }
          if (fields.containsKey("remoteEndpoint.serviceName")) {
            remoteBuilder.serviceName(fields.get("remoteEndpoint.serviceName").get(0));
            hasRemote = true;
          }

          if (hasLocal)
            spanBuilder.localEndpoint(localBuilder.build());
          if (hasRemote)
            spanBuilder.localEndpoint(remoteBuilder.build());

          // tags and annotations
          fields.forEach((k, v) -> {
            if (k.startsWith("tags.")) {
              spanBuilder.putTag(k.substring(5), v.get(0));
            } else if (k.startsWith("annotations.")) {
              spanBuilder.addAnnotation(Long.valueOf(v.get(0)), k.substring(12));
            }
          });

          spans.add(spanBuilder.build());

        } catch (Exception e) {
          e.printStackTrace();
        }
      });

      return spans;
    }
  };

  private RapidRequest getRapidRequest() {
    RapidRequest req = new RapidRequest();
    req.tableNames.add(tableName);
    req.disableHighlight = true;
    req.timeRangeFilter = new TimeRangeFilter(0,
      TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
    req.numResults = maxResultsNum;
    return req;
  }

  private static List<List<Span>> groupByTraceId(Collection<Span> input, boolean strictTraceId) {
    if (input.isEmpty()) return Collections.emptyList();

    Map<String, List<Span>> groupedByTraceId = new HashMap<>();
    for (Span span : input) {
      String traceId = strictTraceId || span.traceId().length() == 16
        ? span.traceId()
        : span.traceId().substring(16);
      if (!groupedByTraceId.containsKey(traceId)) {
        groupedByTraceId.put(traceId, new ArrayList<>());
      }
      groupedByTraceId.get(traceId).add(span);
    }
    return new ArrayList<>(groupedByTraceId.values());
  }
}
