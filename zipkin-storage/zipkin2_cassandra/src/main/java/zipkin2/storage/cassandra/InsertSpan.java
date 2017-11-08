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
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.auto.value.AutoValue;
import com.google.common.base.Joiner;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.internal.Nullable;
import zipkin2.storage.cassandra.Schema.AnnotationUDT;
import zipkin2.storage.cassandra.Schema.EndpointUDT;
import zipkin2.storage.cassandra.internal.call.ResultSetFutureCall;

@AutoValue
abstract class InsertSpan extends ResultSetFutureCall {
  static final Joiner COMMA_JOINER = Joiner.on(',');

  static class Factory {
    final Session session;
    final PreparedStatement preparedStatement;
    final boolean strictTraceId;

    Factory(Session session, boolean strictTraceId) {
      this.session = session;
      this.preparedStatement = session.prepare(
        QueryBuilder
          .insertInto(Schema.TABLE_SPAN)
          .value("trace_id", QueryBuilder.bindMarker("trace_id"))
          .value("trace_id_high", QueryBuilder.bindMarker("trace_id_high"))
          .value("ts_uuid", QueryBuilder.bindMarker("ts_uuid"))
          .value("parent_id", QueryBuilder.bindMarker("parent_id"))
          .value("id", QueryBuilder.bindMarker("id"))
          .value("kind", QueryBuilder.bindMarker("kind"))
          .value("span", QueryBuilder.bindMarker("span"))
          .value("ts", QueryBuilder.bindMarker("ts"))
          .value("duration", QueryBuilder.bindMarker("duration"))
          .value("l_ep", QueryBuilder.bindMarker("l_ep"))
          .value("l_service", QueryBuilder.bindMarker("l_service"))
          .value("r_ep", QueryBuilder.bindMarker("r_ep"))
          .value("annotations", QueryBuilder.bindMarker("annotations"))
          .value("tags", QueryBuilder.bindMarker("tags"))
          .value("shared", QueryBuilder.bindMarker("shared"))
          .value("debug", QueryBuilder.bindMarker("debug"))
          .value("annotation_query", QueryBuilder.bindMarker("annotation_query")));
      this.strictTraceId = strictTraceId;
    }

    Call<ResultSet> create(Span span, UUID ts_uuid) {
      boolean traceIdHigh = !strictTraceId && span.traceId().length() == 32;

      Builder result = newBuilder().session(session).preparedStatement(preparedStatement)
        .ts_uuid(ts_uuid)
        .trace_id_high(traceIdHigh ? span.traceId().substring(0, 16) : null)
        .trace_id(traceIdHigh ? span.traceId().substring(16) : span.traceId())
        .parent_id(span.parentId())
        .id(span.id())
        .kind(span.kind() != null ? span.kind().name() : null)
        .span(span.name())
        .ts(span.timestamp())
        .duration(span.duration())
        .l_ep(span.localEndpoint() != null ? new EndpointUDT(span.localEndpoint()) : null)
        .r_ep(span.remoteEndpoint() != null ? new EndpointUDT(span.remoteEndpoint()) : null)
        .tags(span.tags())
        .debug(Boolean.TRUE.equals(span.debug()))
        .shared(Boolean.TRUE.equals(span.shared()));

      if (!span.annotations().isEmpty()) {
        List<AnnotationUDT> annotations = span.annotations().stream()
          .map(AnnotationUDT::new)
          .collect(Collectors.toList());
        result.annotations(annotations);
      }
      Set<String> annotationQuery = CassandraUtil.annotationKeys(span);
      if (!annotationQuery.isEmpty()) {
        result.annotation_query(COMMA_JOINER.join(annotationQuery));
      }
      return result.build();
    }
  }

  abstract PreparedStatement preparedStatement();

  abstract UUID ts_uuid();

  abstract String trace_id();

  abstract String id();

  @Nullable abstract String trace_id_high();

  @Nullable abstract String parent_id();

  @Nullable abstract String kind();

  @Nullable abstract String span();

  @Nullable abstract Long ts();

  @Nullable abstract Long duration();

  @Nullable abstract EndpointUDT l_ep();

  @Nullable abstract EndpointUDT r_ep();

  abstract List<AnnotationUDT> annotations();

  abstract Map<String, String> tags();

  @Nullable abstract String annotation_query();

  abstract boolean shared();

  abstract boolean debug();

  @Override public Statement newStatement() {
    BoundStatement result = preparedStatement().bind()
      .setUUID("ts_uuid", ts_uuid())
      .setString("trace_id", trace_id())
      .setString("id", id());

    // now set the nullable fields
    if (null != trace_id_high()) result.setString("trace_id_high", trace_id_high());
    if (null != parent_id()) result.setString("parent_id", parent_id());
    if (null != kind()) result.setString("kind", kind());
    if (null != span()) result.setString("span", span());
    if (null != ts()) result.setLong("ts", ts());
    if (null != duration()) result.setLong("duration", duration());
    if (null != l_ep()) result.set("l_ep", l_ep(), EndpointUDT.class);
    if (null != l_ep()) result.setString("l_service", l_ep().getService());
    if (null != r_ep()) result.set("r_ep", r_ep(), EndpointUDT.class);
    if (!annotations().isEmpty()) result.setList("annotations", annotations());
    if (!tags().isEmpty()) result.setMap("tags", tags());
    if (null != annotation_query()) result.setString("annotation_query", annotation_query());
    if (shared()) result.setBool("shared", true);
    if (debug()) result.setBool("debug", true);
    return result;
  }

  @Override public InsertSpan clone() {
    return toBuilder().build();
  }

  abstract Builder toBuilder();

  static Builder newBuilder() {
    return new AutoValue_InsertSpan.Builder()
      .annotations(Collections.emptyList())
      .tags(Collections.emptyMap());
  }

  @AutoValue.Builder
  static abstract class Builder {
    abstract Builder session(Session session);

    abstract Builder preparedStatement(PreparedStatement preparedStatement);

    abstract Builder ts_uuid(UUID ts_uuid);

    abstract Builder trace_id(String trace_id);

    abstract Builder id(String id);

    abstract Builder trace_id_high(@Nullable String trace_id_high);

    abstract Builder parent_id(@Nullable String parent_id);

    abstract Builder kind(@Nullable String kind);

    abstract Builder span(@Nullable String span);

    abstract Builder ts(@Nullable Long ts);

    abstract Builder duration(@Nullable Long duration);

    abstract Builder l_ep(@Nullable EndpointUDT l_ep);

    abstract Builder r_ep(@Nullable EndpointUDT r_ep);

    abstract Builder annotations(List<AnnotationUDT> annotations);

    abstract Builder tags(Map<String, String> tags);

    abstract Builder annotation_query(@Nullable String annotation_query);

    abstract Builder shared(boolean shared);

    abstract Builder debug(boolean debug);

    abstract InsertSpan build();
  }
}
