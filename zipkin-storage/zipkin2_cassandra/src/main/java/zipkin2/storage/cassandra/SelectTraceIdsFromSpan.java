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
import com.google.auto.value.AutoValue;
import java.util.AbstractMap;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import zipkin2.Call;
import zipkin2.storage.cassandra.CassandraSpanStore.TimestampRange;
import zipkin2.storage.cassandra.internal.call.AccumulateAllResults;
import zipkin2.storage.cassandra.internal.call.ResultSetFutureCall;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static zipkin2.storage.cassandra.Schema.TABLE_SPAN;

/** Selects from the {@link Schema#TABLE_SPAN} using data in the partition key or SASI indexes */
abstract class SelectTraceIdsFromSpan extends ResultSetFutureCall {
  static class Factory {
    final Session session;
    final PreparedStatement all, withAnnotationQuery;

    Factory(Session session) {
      this.session = session;
      this.all = session.prepare(
        QueryBuilder.select("ts", "trace_id").from(TABLE_SPAN)
          .where(QueryBuilder.gte("ts_uuid", bindMarker("start_ts")))
          .and(QueryBuilder.lte("ts_uuid", bindMarker("end_ts")))
          .limit(bindMarker("limit_"))
          .allowFiltering());
      this.withAnnotationQuery = session.prepare(
        QueryBuilder.select("ts", "trace_id").from(TABLE_SPAN)
          .where(QueryBuilder.eq("l_service", bindMarker("l_service")))
          .and(QueryBuilder.like("annotation_query", bindMarker("annotation_query")))
          .and(QueryBuilder.gte("ts_uuid", bindMarker("start_ts")))
          .and(QueryBuilder.lte("ts_uuid", bindMarker("end_ts")))
          .limit(bindMarker("limit_"))
          .allowFiltering());
    }

    Call<Set<Entry<String, Long>>> newCall(TimestampRange timestampRange, int limit) {
      return new AutoValue_SelectTraceIdsFromSpan_All(
        session,
        all,
        timestampRange.startUUID,
        timestampRange.endUUID,
        limit
      ).flatMap(new AccumulateTraceIdTsLong());
    }

    Call<Set<Entry<String, Long>>> newCall(
      String serviceName,
      String annotationKey,
      TimestampRange timestampRange,
      int limit
    ) {
      return new AutoValue_SelectTraceIdsFromSpan_ByAnnotationQuery(
        session,
        withAnnotationQuery,
        timestampRange.startUUID,
        timestampRange.endUUID,
        limit,
        serviceName,
        "%" + annotationKey + "%"
      ).flatMap(new AccumulateTraceIdTsLong());
    }
  }

  abstract PreparedStatement preparedStatement();

  abstract UUID start_ts();

  abstract UUID end_ts();

  abstract int limit_();

  @AutoValue
  static abstract class ByAnnotationQuery extends SelectTraceIdsFromSpan {

    abstract String l_service();

    abstract String annotation_query();

    @Override public Statement newStatement() {
      return preparedStatement().bind()
        .setString("l_service", l_service())
        .setString("annotation_query", annotation_query())
        .setUUID("start_ts", start_ts())
        .setUUID("end_ts", end_ts())
        .setInt("limit_", limit_())
        .setFetchSize(limit_());
    }

    @Override public SelectTraceIdsFromSpan clone() {
      return new AutoValue_SelectTraceIdsFromSpan_ByAnnotationQuery(
        session(),
        preparedStatement(),
        start_ts(),
        end_ts(),
        limit_(),
        l_service(),
        annotation_query()
      );
    }

    @Override
    public String toString() {
      return "SelectTraceIdsFromSpan{"
        + "l_service=" + l_service() + ", "
        + "annotation_query=" + annotation_query() + ", "
        + "start_ts=" + start_ts() + ", "
        + "end_ts=" + end_ts() + ", "
        + "limit=" + limit_()
        + "}";
    }
  }

  @AutoValue
  static abstract class All extends SelectTraceIdsFromSpan {

    @Override public Statement newStatement() {
      return preparedStatement().bind()
        .setUUID("start_ts", start_ts())
        .setUUID("end_ts", end_ts())
        .setInt("limit_", limit_())
        .setFetchSize(limit_());
    }

    @Override public SelectTraceIdsFromSpan clone() {
      return new AutoValue_SelectTraceIdsFromSpan_All(
        session(),
        preparedStatement(),
        start_ts(),
        end_ts(),
        limit_()
      );
    }

    @Override
    public String toString() {
      return "SelectTraceIdsFromSpan{"
        + "start_ts=" + start_ts() + ", "
        + "end_ts=" + end_ts() + ", "
        + "limit_=" + limit_()
        + "}";
    }
  }

  static final class AccumulateTraceIdTsLong
    extends AccumulateAllResults<Set<Entry<String, Long>>> {

    @Override protected Supplier<Set<Entry<String, Long>>> supplier() {
      return LinkedHashSet::new; // because results are not distinct
    }

    @Override protected BiConsumer<Row, Set<Entry<String, Long>>> accumulator() {
      return (row, result) -> {
        if (row.isNull("ts")) return;
        result.add(new AbstractMap.SimpleEntry<>(
          row.getString("trace_id"), row.getLong("ts")
        ));
      };
    }
  }
}
