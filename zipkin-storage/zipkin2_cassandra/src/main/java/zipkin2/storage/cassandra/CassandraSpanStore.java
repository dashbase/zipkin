/**
 * Copyright 2015-2017 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.storage.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.Call.FlatMapper;
import zipkin2.Call.Mapper;
import zipkin2.Callback;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.internal.DependencyLinker;
import zipkin2.internal.Nullable;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;
import zipkin2.storage.cassandra.Schema.AnnotationUDT;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.DiscreteDomain.integers;
import static zipkin2.storage.cassandra.CassandraUtil.traceIdsSortedByDescTimestamp;
import static zipkin2.storage.cassandra.Schema.TABLE_DEPENDENCY;
import static zipkin2.storage.cassandra.Schema.TABLE_SERVICE_SPANS;
import static zipkin2.storage.cassandra.Schema.TABLE_SPAN;
import static zipkin2.storage.cassandra.Schema.TABLE_TRACE_BY_SERVICE_SPAN;

final class CassandraSpanStore implements SpanStore {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraSpanStore.class);

  static final Call<List<String>> EMPTY_LIST = Call.emptyList();

  private final int maxTraceCols;
  private final int indexFetchMultiplier;
  private final boolean strictTraceId;
  private final Session session;
  private final PreparedStatement selectTraces;
  private final PreparedStatement selectDependencies;
  private final PreparedStatement selectServiceNames;
  private final PreparedStatement selectSpanNames;
  private final PreparedStatement selectTraceIdsByServiceSpanName;
  private final PreparedStatement selectTraceIdsByServiceSpanNameAndDuration;
  private final PreparedStatement selectTraceIdsByAnnotation;
  private final Mapper<Row, Map.Entry<String, Long>> traceIdToTimestamp;
  private final Mapper<Row, Map.Entry<String, Long>> traceIdToLong;
  private final Mapper<Row, String> rowToSpanName;
  private final Mapper<Row, String> rowToServiceName;
  private final Mapper<Row, Span> rowToSpan;
  private final Mapper<List<Map.Entry<String, Long>>, Map<String, Long>> collapseToMap;
  private final int indexTtl;

  CassandraSpanStore(CassandraStorage storage) {
    session = storage.session();
    maxTraceCols = storage.maxTraceCols();
    indexFetchMultiplier = storage.indexFetchMultiplier();
    strictTraceId = storage.strictTraceId();

    selectTraces = session.prepare(
        QueryBuilder.select(
                "trace_id_high", "trace_id", "parent_id", "id", "kind", "span", "ts",
                "duration", "l_ep", "r_ep", "annotations", "tags", "shared", "debug")
            .from(TABLE_SPAN)
            .where(QueryBuilder.in("trace_id", QueryBuilder.bindMarker("trace_id")))
            .limit(QueryBuilder.bindMarker("limit_")));

    selectDependencies = session.prepare(
        QueryBuilder.select("parent", "child", "errors", "calls")
            .from(TABLE_DEPENDENCY)
            .where(QueryBuilder.in("day", QueryBuilder.bindMarker("days"))));

    selectServiceNames = session.prepare(
        QueryBuilder.select("service")
            .distinct()
            .from(TABLE_SERVICE_SPANS));

    selectSpanNames = session.prepare(
        QueryBuilder.select("span")
            .from(TABLE_SERVICE_SPANS)
            .where(QueryBuilder.eq("service", QueryBuilder.bindMarker("service")))
            .limit(QueryBuilder.bindMarker("limit_")));

    selectTraceIdsByServiceSpanName = session.prepare(
        QueryBuilder.select("ts", "trace_id")
            .from(TABLE_TRACE_BY_SERVICE_SPAN)
            .where(QueryBuilder.eq("service", QueryBuilder.bindMarker("service")))
            .and(QueryBuilder.eq("span", QueryBuilder.bindMarker("span")))
            .and(QueryBuilder.eq("bucket", QueryBuilder.bindMarker("bucket")))
            .and(QueryBuilder.gte("ts", QueryBuilder.bindMarker("start_ts")))
            .and(QueryBuilder.lte("ts", QueryBuilder.bindMarker("end_ts")))
            .limit(QueryBuilder.bindMarker("limit_")));

    selectTraceIdsByServiceSpanNameAndDuration = session.prepare(
        QueryBuilder.select("ts", "trace_id")
            .from(TABLE_TRACE_BY_SERVICE_SPAN)
            .where(QueryBuilder.eq("service", QueryBuilder.bindMarker("service")))
            .and(QueryBuilder.eq("span", QueryBuilder.bindMarker("span")))
            .and(QueryBuilder.eq("bucket", QueryBuilder.bindMarker("bucket")))
            .and(QueryBuilder.gte("ts", QueryBuilder.bindMarker("start_ts")))
            .and(QueryBuilder.lte("ts", QueryBuilder.bindMarker("end_ts")))
            .and(QueryBuilder.gte("duration", QueryBuilder.bindMarker("start_duration")))
            .and(QueryBuilder.lte("duration", QueryBuilder.bindMarker("end_duration")))
            .limit(QueryBuilder.bindMarker("limit_")));

    selectTraceIdsByAnnotation = session.prepare(
        QueryBuilder.select("ts", "trace_id")
            .from(TABLE_SPAN)
            .where(QueryBuilder.eq("l_service", QueryBuilder.bindMarker("l_service")))
            .and(QueryBuilder.like("annotation_query", QueryBuilder.bindMarker("annotation_query")))
            .and(QueryBuilder.gte("ts_uuid", QueryBuilder.bindMarker("start_ts")))
            .and(QueryBuilder.lte("ts_uuid", QueryBuilder.bindMarker("end_ts")))
            .limit(QueryBuilder.bindMarker("limit_"))
            .allowFiltering());

    traceIdToTimestamp = row ->
      new AbstractMap.SimpleEntry<>(
          row.getString("trace_id"),
          UUIDs.unixTimestamp(row.getUUID("ts")));

    traceIdToLong = row ->
        new AbstractMap.SimpleEntry<>(
            row.getString("trace_id"),
            row.getLong("ts"));

    rowToSpanName = row -> row.getString("span");

    rowToServiceName = row -> row.getString("service");

    rowToSpan = row -> {
      String traceId = row.getString("trace_id");
      if (!strictTraceId) {
        String traceIdHigh = row.getString("trace_id_high");
        if (traceIdHigh != null) traceId = traceIdHigh + traceId;
      }
      Span.Builder builder = Span.newBuilder()
          .traceId(traceId)
          .parentId(row.getString("parent_id"))
          .id(row.getString("id"))
          .name(row.getString("span"))
          .timestamp(row.getLong("ts"));

      if (!row.isNull("duration")) {
        builder.duration(row.getLong("duration"));
      }
      if (!row.isNull("kind")) {
        try {
          builder.kind(Span.Kind.valueOf(row.getString("kind")));
        } catch (IllegalArgumentException ignored) {
        }
      }
      if (!row.isNull("l_ep")) {
        builder = builder.localEndpoint(row.get("l_ep", Schema.EndpointUDT.class).toEndpoint());
      }
      if (!row.isNull("r_ep")) {
        builder = builder.remoteEndpoint(row.get("r_ep", Schema.EndpointUDT.class).toEndpoint());
      }
      if (!row.isNull("shared")) {
        builder = builder.shared(row.getBool("shared"));
      }
      if (!row.isNull("debug")) {
        builder = builder.shared(row.getBool("debug"));
      }
      for (AnnotationUDT udt : row.getList("annotations", AnnotationUDT.class)) {
        builder = builder.addAnnotation(udt.toAnnotation().timestamp(), udt.toAnnotation().value());
      }
      for (Map.Entry<String, String> tag : row.getMap("tags", String.class, String.class)
        .entrySet()) {
        builder = builder.putTag(tag.getKey(), tag.getValue());
      }
      return builder.build();
    };

    collapseToMap = input -> {
      Map<String, Long> result = new LinkedHashMap<>();
      input.forEach(m -> result.put(m.getKey(), m.getValue()));
      return result;
    };

    KeyspaceMetadata md = Schema.getKeyspaceMetadata(session);
    this.indexTtl = md.getTable(TABLE_TRACE_BY_SERVICE_SPAN).getOptions().getDefaultTimeToLive();
  }

  /**
   * This fans out into a number of requests. The returned future will fail if any of the
   * inputs fail.
   *
   * <p>When {@link QueryRequest#serviceName service name} is unset, service names will be
   * fetched eagerly, implying an additional query.
   *
   * <p>The duration query is the most expensive query in cassandra, as it turns into 1 request per
   * hour of {@link QueryRequest#lookback lookback}. Because many times lookback is set to a day,
   * this means 24 requests to the backend!
   *
   * <p>See https://github.com/openzipkin/zipkin-java/issues/200
   */
  @Override
  public Call<List<List<Span>>> getTraces(final QueryRequest request) {
    // Over fetch on indexes as they don't return distinct (trace id, timestamp) rows.
    final int traceIndexFetchSize = request.limit() * indexFetchMultiplier;
    Call<List<Map.Entry<String, Long>>> traceIdToTimestamp = getTraceIdsByServiceNames(request);
    List<String> annotationKeys = CassandraUtil.annotationKeys(request);
    Call<List<String>> traceIds;
    if (annotationKeys.isEmpty()) {
      // Simplest case is when there is no annotation query. Limit is valid since there's no AND
      // query that could reduce the results returned to less than the limit.
      traceIds = traceIdToTimestamp.map(collapseToMap).map(traceIdsSortedByDescTimestamp());
    } else {
      // While a valid port of the scala cassandra span store (from zipkin 1.35), there is a fault.
      // each annotation key is an intersection, meaning we likely return < traceIndexFetchSize.
      List<Call<Map<String, Long>>> futureKeySetsToIntersect = new ArrayList<>();
      if (request.spanName() != null) {
        futureKeySetsToIntersect.add(traceIdToTimestamp.map(collapseToMap));
      }
      for (String annotationKey : annotationKeys) {
        futureKeySetsToIntersect
          .add(
            getTraceIdsByAnnotation(request, annotationKey, request.endTs(), traceIndexFetchSize));
      }
      // We achieve the AND goal, by intersecting each of the key sets.
      traceIds = new IntersectKeySets<>(futureKeySetsToIntersect);
      // @xxx the sorting by timestamp desc is broken here^
    }

    return traceIds.flatMap(new FlatMapper<List<String>, List<List<Span>>>() {
      @Override public Call<List<List<Span>>> map(List<String> traceIds) {
        if (traceIds.size() > request.limit()) {
          traceIds = traceIds.subList(0, request.limit());
        }
        return getSpansByTraceIds(traceIds, maxTraceCols).map(input -> {
          List<List<Span>> traces = groupByTraceId(input, strictTraceId);
          // Due to tokenization of the trace ID, our matches are imprecise on Span.traceIdHigh
          for (Iterator<List<Span>> trace = traces.iterator(); trace.hasNext(); ) {
            List<Span> next = trace.next();
            if (next.get(0).traceId().length() > 16 && !request.test(next)) {
              trace.remove();
            }
          }
          return traces;
        });
      }

      @Override public String toString() {
        return "getSpansByTraceIds";
      }
    });
  }

  @Override public Call<List<Span>> getTrace(String traceId) {
    // make sure we have a 16 or 32 character trace ID
    traceId = Span.normalizeTraceId(traceId);

    // Unless we are strict, truncate the trace ID to 64bit (encoded as 16 characters)
    if (!strictTraceId && traceId.length() == 32) traceId = traceId.substring(16);

    List<String> traceIds = Collections.singletonList(traceId);
    return getSpansByTraceIds(traceIds, maxTraceCols);
  }

  @Override public Call<List<String>> getServiceNames() {
    BoundStatement bound = CassandraUtil.bindWithName(selectServiceNames, "select-service-names");
    return new ListenableFutureCall<ResultSet>() {
      @Override protected ListenableFuture<ResultSet> newFuture() {
        return session.executeAsync(bound);
      }
    }.flatMap(new AccumulateIntoList<>(rowToServiceName));
  }

  @Override public Call<List<String>> getSpanNames(String serviceName) {
    if (serviceName == null || serviceName.isEmpty()) return EMPTY_LIST;
    serviceName = checkNotNull(serviceName, "serviceName").toLowerCase();

    BoundStatement bound = CassandraUtil.bindWithName(selectSpanNames, "select-span-names")
      .setString("service", serviceName)
      // no one is ever going to browse so many span names
      .setInt("limit_", 1000);

    return new ListenableFutureCall<ResultSet>() {
      @Override protected ListenableFuture<ResultSet> newFuture() {
        return session.executeAsync(bound);
      }
    }.flatMap(new AccumulateIntoList<>(rowToSpanName));
  }

  @Override public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    List<LocalDate> days = CassandraUtil.getDays(endTs, lookback);

    BoundStatement bound = CassandraUtil
      .bindWithName(selectDependencies, "select-dependencies")
      .setList("days", days);

    return new ListenableFutureCall<ResultSet>() {
      @Override protected ListenableFuture<ResultSet> newFuture() {
        return session.executeAsync(bound);
      }
    }.map(ConvertDependenciesResponse.INSTANCE);
  }

  enum ConvertDependenciesResponse implements Mapper<ResultSet, List<DependencyLink>> {
    INSTANCE;

    @Override public List<DependencyLink> map(@Nullable ResultSet rs) {
      ImmutableList.Builder<DependencyLink> unmerged = ImmutableList.builder();
      for (Row row : rs) {
        unmerged.add(
                DependencyLink.newBuilder()
                .parent(row.getString("parent"))
                .child(row.getString("child"))
                .errorCount(row.getLong("errors"))
                .callCount(row.getLong("calls"))
                .build());
      }
      return DependencyLinker.merge(unmerged.build());
    }
  }

  /**
   * Get the available trace information from the storage system. Spans in trace should be sorted by
   * the first annotation timestamp in that span. First event should be first in the spans list. <p>
   * The return list will contain only spans that have been found, thus the return list may not
   * match the provided list of ids.
   */
  Call<List<Span>> getSpansByTraceIds(List<String> traceIds, int limit) {
    checkNotNull(traceIds, "traceIds");
    if (traceIds.isEmpty()) return Call.emptyList();

    Statement bound = CassandraUtil.bindWithName(selectTraces, "select-span")
      .setList("trace_id", traceIds)
      .setInt("limit_", limit);

    return new ListenableFutureCall<ResultSet>() {
      @Override protected ListenableFuture<ResultSet> newFuture() {
        return session.executeAsync(bound);
      }
    }.flatMap(new AccumulateIntoList<>(rowToSpan));
  }

  Call<List<Map.Entry<String, Long>>> getTraceIdsByServiceNames(QueryRequest request) {
    long oldestData = Math.max(System.currentTimeMillis() - indexTtl * 1000, 0); // >= 1970
    long startTsMillis = Math.max((request.endTs() - request.lookback()), oldestData);
    long endTsMillis = Math.max(request.endTs(), oldestData);

    Call<List<String>> serviceNames;
    if (null != request.serviceName()) {
      serviceNames = Call.create(Collections.singletonList(request.serviceName()));
    } else {
      serviceNames = getServiceNames();
    }

    int startBucket = CassandraUtil.durationIndexBucket(startTsMillis * 1000);
    int endBucket = CassandraUtil.durationIndexBucket(endTsMillis * 1000);
    if (startBucket > endBucket) {
      throw new IllegalArgumentException(
        "Start bucket (" + startBucket + ") > end bucket (" + endBucket + ")");
    }
    Set<Integer> buckets = ContiguousSet.create(Range.closed(startBucket, endBucket), integers());
    boolean withDuration = null != request.minDuration() || null != request.maxDuration();

    return serviceNames.flatMap(serviceNames1 -> {
      List<Call<List<Map.Entry<String, Long>>>> calls = new ArrayList<>();

      if (200 < serviceNames1.size() * buckets.size()) {
        LOG.warn("read against " + TABLE_TRACE_BY_SERVICE_SPAN
          + " fanning out to " + serviceNames1.size() * buckets.size() + " requests");
        //@xxx the fan-out of requests here can be improved
      }

      for (String serviceName : serviceNames1) {
        for (Integer bucket : buckets) {
          BoundStatement bound = CassandraUtil
              .bindWithName(
                  withDuration
                      ? selectTraceIdsByServiceSpanNameAndDuration
                      : selectTraceIdsByServiceSpanName,
                  "select-trace-ids-by-service-name")
              .setString("service", serviceName)
              .setString("span", null != request.spanName() ? request.spanName() : "")
              .setInt("bucket", bucket)
              .setUUID("start_ts", UUIDs.startOf(startTsMillis))
              .setUUID("end_ts", UUIDs.endOf(endTsMillis))
              .setInt("limit_", request.limit());

          if (withDuration) {
            long minDuration = TimeUnit.MICROSECONDS
                    .toMillis(null != request.minDuration() ? request.minDuration() : 0);

            long maxDuration = TimeUnit.MICROSECONDS
                    .toMillis(null != request.maxDuration() ? request.maxDuration() : Long.MAX_VALUE);

            bound = bound.setLong("start_duration", minDuration).setLong("end_duration", maxDuration);
          }
          bound.setFetchSize(request.limit());

          BoundStatement finalBound = bound;
          calls.add(new ListenableFutureCall<ResultSet>() {
            @Override protected ListenableFuture newFuture() {
              return session.executeAsync(finalBound);
            }
          }.flatMap(new AccumulateIntoList(traceIdToTimestamp)));
        }
      }

      return new AggregateIntoList<>(calls);
    });
  }

  Call<Map<String, Long>> getTraceIdsByAnnotation(
    QueryRequest request,
    String annotationKey,
    long endTsMillis,
    int limit) {

    long lookbackMillis = request.lookback();
    long oldestData = Math.max(System.currentTimeMillis() - indexTtl * 1000, 0); // >= 1970
    long startTsMillis = Math.max((endTsMillis - lookbackMillis), oldestData);
    endTsMillis = Math.max(endTsMillis, oldestData);

    BoundStatement bound =
      CassandraUtil.bindWithName(selectTraceIdsByAnnotation, "select-trace-ids-by-annotation")
          .setString("l_service", request.serviceName())
          .setString("annotation_query", "%" + annotationKey + "%")
          .setUUID("start_ts", UUIDs.startOf(startTsMillis))
          .setUUID("end_ts", UUIDs.endOf(endTsMillis))
          .setInt("limit_", limit);

    return new ListenableFutureCall<ResultSet>() {
      @Override protected ListenableFuture<ResultSet> newFuture() {
        return session.executeAsync(bound);
      }
    }.flatMap(new AccumulateIntoList(traceIdToLong)).map(collapseToMap);
  }

  static class AccumulateIntoList<T> implements FlatMapper<ResultSet, List<T>> {
    final ImmutableSet.Builder<T> builder = ImmutableSet.builder();
    final Mapper<Row, T> rowMapper;

    AccumulateIntoList(Mapper<Row, T> rowMapper) {
      this.rowMapper = rowMapper;
    }

    @Override public Call<List<T>> map(ResultSet rs) {
      if (!rs.isFullyFetched()) rs.fetchMoreResults(); // TODO: dropped future
      for (Row row : rs) {
        builder.add(rowMapper.map(row));
        if (2000 == rs.getAvailableWithoutFetching() && !rs.isFullyFetched()) {
          rs.fetchMoreResults();
        }
        if (0 == rs.getAvailableWithoutFetching()) break;
      }
      if (rs.getExecutionInfo().getPagingState() == null) {
        return Call.create(ImmutableList.copyOf(builder.build()));
      }
      return new ListenableFutureCall<ResultSet>() {
        @Override protected ListenableFuture<ResultSet> newFuture() {
          return rs.fetchMoreResults();
        }
      }.flatMap(this);
    }
  }

  // TODO(adriancole): at some later point we can refactor this out
  static List<List<Span>> groupByTraceId(Collection<Span> input, boolean strictTraceId) {
    if (input.isEmpty()) return Collections.emptyList();

    Map<String, List<Span>> groupedByTraceId = new LinkedHashMap<>();
    for (Span span : input) {
      String traceId = strictTraceId || span.traceId().length() == 16
        ? span.traceId()
        : span.traceId().substring(16);
      if (!groupedByTraceId.containsKey(traceId)) {
        groupedByTraceId.put(traceId, new LinkedList<>());
      }
      groupedByTraceId.get(traceId).add(span);
    }
    return new ArrayList<>(groupedByTraceId.values());
  }

  static final class IntersectKeySets<K> extends AggregateCall<Map<K, Long>, List<K>> {

    IntersectKeySets(List<Call<Map<K, Long>>> calls) {
      super(calls);
    }

    @Override List<K> newOutput() {
      return new ArrayList<>();
    }

    @Override void append(Map<K, Long> input, List<K> output) {
      if (output.isEmpty()) {
        output.addAll(input.keySet());
      } else {
        output.retainAll(input.keySet());
      }
    }

    @Override boolean isEmpty(List<K> output) {
      return output.isEmpty();
    }

    @Override public Call<List<K>> clone() {
      return new IntersectKeySets<>(calls); // TODO: clone calls
    }
  }

  static final class AggregateIntoList<T> extends AggregateCall<List<T>, List<T>> {
    AggregateIntoList(List<Call<List<T>>> calls) {
      super(calls);
    }

    @Override List<T> newOutput() {
      return new ArrayList<>();
    }

    @Override void append(List<T> input, List<T> output) {
      output.addAll(input);
    }

    @Override boolean isEmpty(List<T> output) {
      return output.isEmpty();
    }

    @Override public AggregateIntoList<T> clone() {
      return new AggregateIntoList<>(calls); // TODO: clone the calls
    }
  }

  static abstract class AggregateCall<I, O> extends Call.Base<O> {
    final List<Call<I>> calls;

    AggregateCall(List<Call<I>> calls) {
      this.calls = calls;
    }  // TODO: one day we could make this cancelable

    abstract O newOutput();

    abstract void append(I input, O output);

    abstract boolean isEmpty(O output);

    @Override protected O doExecute() throws IOException {
      O result = newOutput();
      IOException ioe = null;
      RuntimeException rte = null;
      for (Call<I> call : calls) {
        try {
          append(call.execute(), result);
        } catch (IOException e) {
          ioe = e;
        } catch (RuntimeException e) {
          rte = e;
        }
      }
      if (isEmpty(result)) {
        if (ioe != null) throw ioe;
        if (rte != null) throw rte;
        return result;
      }
      return result;
    }

    @Override protected void doEnqueue(Callback<O> callback) {
      // TODO(adrian): this is terrible for performance, make this submit for each call and gather
      new Thread(() -> {
        try {
          callback.onSuccess(doExecute());
        } catch (Throwable t) {
          callback.onError(t);
        }
      }).start();
    }
  }
}
