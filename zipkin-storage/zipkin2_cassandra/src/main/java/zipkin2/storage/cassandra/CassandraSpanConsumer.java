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
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import zipkin2.Annotation;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.cassandra.internal.call.AggregateCall;

import static zipkin2.storage.cassandra.CassandraUtil.durationIndexBucket;

class CassandraSpanConsumer implements SpanConsumer { // not final for testing
  static final Joiner COMMA_JOINER = Joiner.on(',');

  private static final long WRITTEN_NAMES_TTL
    = Long.getLong("zipkin2.storage.cassandra.internal.writtenNamesTtl", 60 * 60 * 1000);

  private final Session session;
  private final boolean strictTraceId;
  private final InsertSpan.Factory insertSpan;
  private final InsertTraceByServiceSpan.Factory insertTraceServiceSpanName;
  private final InsertServiceSpan.Factory insertServiceSpanName;

  CassandraSpanConsumer(CassandraStorage storage) {
    session = storage.session();
    strictTraceId = storage.strictTraceId();

    // warns when schema problems exist
    Schema.readMetadata(session);

    insertSpan = new InsertSpan.Factory(session, strictTraceId);
    insertTraceServiceSpanName = new InsertTraceByServiceSpan.Factory(session);
    insertServiceSpanName = new InsertServiceSpan.Factory(session, WRITTEN_NAMES_TTL);
  }

  /**
   * This fans out into many requests, last count was 2 * spans.size. If any of these fail, the
   * returned future will fail. Most callers drop or log the result.
   */
  @Override
  public Call<Void> accept(List<Span> spans) {
    if (spans.isEmpty()) return Call.create(null);

    List<Call<ResultSet>> calls = new ArrayList<>();
    for (Span s : spans) {
      // indexing occurs by timestamp, so derive one if not present.
      long ts_micro = s.timestamp() != null ? s.timestamp() : guessTimestamp(s);

      // fallback to current time on the ts_uuid for span data, so we know when it was inserted
      UUID ts_uuid = new UUID(
        UUIDs.startOf(ts_micro != 0L ? (ts_micro / 1000L) : System.currentTimeMillis())
          .getMostSignificantBits(),
        UUIDs.random().getLeastSignificantBits());

      calls.add(insertSpan.create(s, ts_uuid));

      // Empty values allow for api queries with blank service or span name
      String service = s.localServiceName() != null ? s.localServiceName() : "";
      String span = null != s.name() ? s.name() : "";

      // service span index is refreshed regardless of timestamp
      if (null != s.remoteServiceName()) { // allows getServices to return remote service names
        calls.add(insertServiceSpanName.create(s.remoteServiceName(), span));
      }

      if (!service.equals("")) {
        calls.add(insertServiceSpanName.create(service, span));
      }

      if (ts_micro == 0L) continue; // search is only valid with a timestamp, don't index w/o it!
      int bucket = durationIndexBucket(ts_micro); // duration index is milliseconds not microseconds
      Long duration = null != s.duration() ? TimeUnit.MICROSECONDS.toMillis(s.duration()) : null;
      calls.add(
        insertTraceServiceSpanName.create(service, span, bucket, ts_uuid, s.traceId(), duration)
      );
      if (!service.isEmpty()) {
        calls.add( // Allows lookup without the service name
          insertTraceServiceSpanName.create("", span, bucket, ts_uuid, s.traceId(), duration)
        );
      }
      if (span.isEmpty()) continue;
      calls.add( // Allows lookup without the span name
        insertTraceServiceSpanName.create(service, "", bucket, ts_uuid, s.traceId(), duration)
      );
      if (!service.isEmpty()) {
        calls.add( // Allows lookup without the service name
          insertTraceServiceSpanName.create("", "", bucket, ts_uuid, s.traceId(), duration)
        );
      }
    }
    return new StoreSpansCall(calls);
  }

  private static long guessTimestamp(Span span) {
    Preconditions.checkState(null == span.timestamp(),
      "method only for when span has no timestamp");
    for (Annotation annotation : span.annotations()) {
      if (0L < annotation.timestamp()) {
        return annotation.timestamp();
      }
    }
    return 0L; // return a timestamp that won't match a query
  }

  static final class StoreSpansCall extends AggregateCall<ResultSet, Void> {
    StoreSpansCall(List<Call<ResultSet>> calls) {
      super(calls);
    }

    volatile boolean empty = true;

    @Override protected Void newOutput() {
      return null;
    }

    @Override protected void append(ResultSet input, Void output) {
      empty = false;
    }

    @Override protected boolean isEmpty(Void output) {
      return empty;
    }

    @Override public StoreSpansCall clone() {
      return new StoreSpansCall(calls); // TODO: clone the calls
    }
  }

  BoundStatement bind(PreparedStatement prepared) { // overridable for tests
    return prepared.bind();
  }
}
