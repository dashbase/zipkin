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
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.auto.value.AutoValue;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.storage.cassandra.internal.call.ResultSetFutureCall;

@AutoValue
abstract class InsertServiceSpan extends ResultSetFutureCall {

  static class Factory {
    final Session session;
    final PreparedStatement preparedStatement;
    final ConcurrentMap<ServiceSpan, InsertServiceSpan> cache;

    Factory(Session session, long ttl) {
      this.session = session;
      this.preparedStatement = session.prepare(QueryBuilder
        .insertInto(Schema.TABLE_SERVICE_SPANS)
        .value("service", QueryBuilder.bindMarker("service"))
        .value("span", QueryBuilder.bindMarker("span"))
      );
      this.cache = CacheBuilder.newBuilder()
        .expireAfterWrite(ttl, TimeUnit.MILLISECONDS)
        .ticker(new Ticker() {
          @Override public long read() {
            return nanoTime();
          }
        })
        // TODO: maximum size or weight
        .<ServiceSpan, InsertServiceSpan>build().asMap();
    }

    // visible for testing, since nanoTime is weird and can return negative
    long nanoTime() {
      return System.nanoTime();
    }

    Call<ResultSet> create(String service, String span) {
      ServiceSpan serviceSpan = ServiceSpan.create(service, span);
      if (cache.containsKey(serviceSpan)) return Call.create(null);
      AutoValue_InsertServiceSpan realCall = new AutoValue_InsertServiceSpan(
        session,
        cache,
        preparedStatement,
        serviceSpan
      );
      if (cache.putIfAbsent(serviceSpan, realCall) != null) return Call.create(null);
      return realCall;
    }
  }

  @AutoValue
  static abstract class ServiceSpan {
    static ServiceSpan create(String service, String span) {
      return new AutoValue_InsertServiceSpan_ServiceSpan(service, span);
    }

    abstract String service();

    abstract String span();
  }

  abstract ConcurrentMap<ServiceSpan, InsertServiceSpan> cache();

  abstract PreparedStatement preparedStatement();

  abstract ServiceSpan serviceSpan();

  @Override public Statement newStatement() {
    return preparedStatement().bind()
      .setString("service", serviceSpan().service())
      .setString("span", serviceSpan().span());
  }

  @Override public InsertServiceSpan clone() {
    return new AutoValue_InsertServiceSpan(
      session(),
      cache(),
      preparedStatement(),
      serviceSpan()
    );
  }

  @Override protected ResultSet doExecute() throws IOException {
    try {
      return super.doExecute();
    } catch (IOException | RuntimeException | Error e) {
      cache().remove(serviceSpan(), this); // invalidate
      throw e;
    }
  }

  @Override protected void doEnqueue(Callback<ResultSet> callback) {
    super.doEnqueue(new Callback<ResultSet>() {
      @Override public void onSuccess(ResultSet value) {
        callback.onSuccess(value);
      }

      @Override public void onError(Throwable t) {
        cache().remove(serviceSpan(), this); // invalidate
        callback.onError(t);
      }
    });
  }

  @Override protected void doCancel() {
    cache().remove(serviceSpan(), this); // invalidate
    super.doCancel();
  }

  @Override
  public String toString() {
    return "InsertServiceSpan{"
      + "service=" + serviceSpan().service() + ", "
      + "span=" + serviceSpan().span()
      + "}";
  }
}
