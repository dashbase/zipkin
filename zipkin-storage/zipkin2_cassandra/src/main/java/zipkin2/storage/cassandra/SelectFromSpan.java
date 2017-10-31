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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.cassandra.internal.call.AccumulateAllResults;
import zipkin2.storage.cassandra.internal.call.ResultSetFutureCall;

import static com.google.common.base.Preconditions.checkNotNull;
import static zipkin2.storage.cassandra.CassandraSpanStore.groupByTraceId;
import static zipkin2.storage.cassandra.Schema.TABLE_SPAN;

abstract class SelectFromSpan extends ResultSetFutureCall {
  static class Factory {
    final Session session;
    final PreparedStatement byTraceIds;
    final AccumulateSpansAllResults accumulateSpans;
    final boolean strictTraceId;
    final int maxTraceCols;

    Factory(Session session, boolean strictTraceId, int maxTraceCols) {
      this.session = session;
      this.accumulateSpans = new AccumulateSpansAllResults(strictTraceId);
      this.byTraceIds = session.prepare(selectFromSpan()
        .where(QueryBuilder.in("trace_id", QueryBuilder.bindMarker("trace_id")))
        .limit(QueryBuilder.bindMarker("limit_"))
      );
      this.strictTraceId = strictTraceId;
      this.maxTraceCols = maxTraceCols;
    }

    Call<List<Span>> newCall(List<String> traceIds, int limit) {
      checkNotNull(traceIds, "traceIds");
      if (traceIds.isEmpty()) return Call.emptyList();
      if (traceIds.size() > limit) {
        traceIds = traceIds.subList(0, limit);
      }
      return new AutoValue_SelectFromSpan_ByTraceId(
        session,
        byTraceIds,
        maxTraceCols, // amount of spans per trace is almost always larger than trace IDs
        traceIds
      ).flatMap(accumulateSpans);
    }

    FlatMapper<Set<String>, List<List<Span>>> newFlatMapper(int limit) {
      return traceIds -> newCall(new ArrayList<>(traceIds), limit)
        .map(spans -> groupByTraceId(spans, strictTraceId));
    }
  }

  abstract PreparedStatement preparedStatement();

  abstract int limit_();

  @AutoValue
  static abstract class ByTraceId extends SelectFromSpan {
    abstract List<String> trace_id();

    @Override public Statement newStatement() {
      return preparedStatement().bind()
        .setList("trace_id", trace_id())
        .setInt("limit_", limit_());
    }

    @Override public SelectFromSpan clone() {
      return new AutoValue_SelectFromSpan_ByTraceId(
        session(),
        preparedStatement(),
        limit_(),
        trace_id()
      );
    }

    @Override
    public String toString() {
      return "SelectFromSpan{"
        + "trace_id=" + trace_id() + ", "
        + "limit_=" + limit_()
        + "}";
    }
  }

  @AutoValue
  static abstract class ByTime extends SelectFromSpan {

    abstract UUID start_ts();

    abstract UUID end_ts();

    @Override public Statement newStatement() {
      return preparedStatement().bind().setUUID("start_ts", start_ts())
        .setUUID("end_ts", end_ts())
        .setInt("limit_", limit_())
        .setFetchSize(limit_());
    }

    @Override public SelectFromSpan clone() {
      return new AutoValue_SelectFromSpan_ByTime(
        session(),
        preparedStatement(),
        limit_(),
        start_ts(),
        end_ts()
      );
    }

    @Override
    public String toString() {
      return "SelectFromSpan{"
        + "start_ts=" + start_ts() + ", "
        + "end_ts=" + end_ts() + ", "
        + "limit_=" + limit_()
        + "}";
    }
  }

  static class AccumulateSpansAllResults extends AccumulateAllResults<List<Span>> {
    final boolean strictTraceId;

    AccumulateSpansAllResults(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
    }

    @Override protected Supplier<List<Span>> supplier() {
      return ArrayList::new;
    }

    @Override protected BiConsumer<Row, List<Span>> accumulator() {
      return (row, result) -> {
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
        for (Schema.AnnotationUDT udt : row.getList("annotations", Schema.AnnotationUDT.class)) {
          builder =
            builder.addAnnotation(udt.toAnnotation().timestamp(), udt.toAnnotation().value());
        }
        for (Entry<String, String> tag : row.getMap("tags", String.class, String.class)
          .entrySet()) {
          builder = builder.putTag(tag.getKey(), tag.getValue());
        }
        result.add(builder.build());
      };
    }
  }

  static Select selectFromSpan() {
    return QueryBuilder.select(
      "trace_id_high", "trace_id", "parent_id", "id", "kind", "span", "ts",
      "duration", "l_ep", "r_ep", "annotations", "tags", "shared", "debug")
      .from(TABLE_SPAN);
  }
}
