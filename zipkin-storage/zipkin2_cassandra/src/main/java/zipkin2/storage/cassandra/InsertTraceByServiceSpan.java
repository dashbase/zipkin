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
import java.util.UUID;
import zipkin2.Call;
import zipkin2.internal.Nullable;
import zipkin2.storage.cassandra.internal.call.ResultSetFutureCall;

@AutoValue
abstract class InsertTraceByServiceSpan extends ResultSetFutureCall {

  static class Factory {
    final Session session;
    final PreparedStatement preparedStatement;

    Factory(Session session) {
      this.session = session;
      this.preparedStatement = session.prepare(
        QueryBuilder
          .insertInto(Schema.TABLE_TRACE_BY_SERVICE_SPAN)
          .value("service", QueryBuilder.bindMarker("service"))
          .value("span", QueryBuilder.bindMarker("span"))
          .value("bucket", QueryBuilder.bindMarker("bucket"))
          .value("ts", QueryBuilder.bindMarker("ts"))
          .value("trace_id", QueryBuilder.bindMarker("trace_id"))
          .value("duration", QueryBuilder.bindMarker("duration")));
    }

    Call<ResultSet> create(
      String service,
      String span,
      int bucket,
      UUID ts,
      String trace_id,
      @Nullable Long durationMillis
    ) {
      return new AutoValue_InsertTraceByServiceSpan(
        session,
        preparedStatement,
        service,
        span,
        bucket,
        ts,
        trace_id,
        durationMillis
      );
    }
  }

  abstract PreparedStatement preparedStatement();

  abstract String service();

  abstract String span();

  abstract int bucket();

  abstract UUID ts();

  abstract String trace_id();

  @Nullable abstract Long duration();

  @Override public Statement newStatement() {
    BoundStatement result = preparedStatement().bind()
      .setString("service", service())
      .setString("span", span())
      .setInt("bucket", bucket())
      .setUUID("ts", ts())
      .setString("trace_id", trace_id());

    if (null != duration()) {
      result.setLong("duration", duration());
    }
    return result;
  }

  @Override public InsertTraceByServiceSpan clone() {
    return new AutoValue_InsertTraceByServiceSpan(
      session(),
      preparedStatement(),
      service(),
      span(),
      bucket(),
      ts(),
      trace_id(),
      duration()
    );
  }
}
