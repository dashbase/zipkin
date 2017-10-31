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
package zipkin2.storage.cassandra.internal.call;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.auto.value.AutoValue;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import zipkin2.Call;
import zipkin2.Call.FlatMapper;

public abstract class AccumulateAllResults<T> implements FlatMapper<ResultSet, T> {
  protected abstract Supplier<T> supplier();

  protected abstract BiConsumer<Row, T> accumulator();

  @Override public Call<T> map(ResultSet rs) {
    return new AutoValue_AccumulateAllResults_AccumulateNextResults<>(
      supplier().get(),
      accumulator()
    ).map(rs);
  }

  @AutoValue
  static abstract class FetchMoreResults extends ListenableFutureCall<ResultSet> {
    abstract ResultSet resultSet();

    @Override protected ListenableFuture<ResultSet> newFuture() {
      return resultSet().fetchMoreResults();
    }

    @Override public Call<ResultSet> clone() {
      throw new UnsupportedOperationException();
    }
  }

  @AutoValue
  static abstract class AccumulateNextResults<T> implements FlatMapper<ResultSet, T> {
    abstract T pendingResults();

    abstract BiConsumer<Row, T> accumulator();

    @Override public Call<T> map(ResultSet rs) {
      if (!rs.isFullyFetched()) rs.fetchMoreResults(); // TODO: dropped future
      for (Row row : rs) {
        accumulator().accept(row, pendingResults());
        // TODO magic number
        if (2000 == rs.getAvailableWithoutFetching() && !rs.isFullyFetched()) {
          rs.fetchMoreResults(); // TODO: dropped future
        }
        if (0 == rs.getAvailableWithoutFetching()) break;
      }
      if (rs.getExecutionInfo().getPagingState() == null) return Call.create(pendingResults());
      return new AutoValue_AccumulateAllResults_FetchMoreResults(rs).flatMap(this);
    }
  }
}
