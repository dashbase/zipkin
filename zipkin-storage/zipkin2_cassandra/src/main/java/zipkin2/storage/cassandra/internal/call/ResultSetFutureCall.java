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
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import java.io.IOException;
import zipkin2.Call;
import zipkin2.Callback;

/**
 * Similar to {@link ListenableFutureCall} except it takes advantage of special 'get' hooks on
 * {@link com.datastax.driver.core.ResultSetFuture}.
 */
// some copy/pasting is ok here as debugging is obscured when the type hierarchy gets deep.
public abstract class ResultSetFutureCall extends Call.Base<ResultSet> {
  public abstract Session session();

  public abstract Statement newStatement();

  /** Defers I/O until {@link #enqueue(Callback)} or {@link #execute()} are called. */
  final ResultSetFuture newFuture() {
    return session().executeAsync(newStatement());
  }

  volatile ResultSetFuture future;

  @Override protected final ResultSet doExecute() throws IOException {
    return (future = newFuture()).getUninterruptibly();
  }

  @Override protected final void doEnqueue(Callback<ResultSet> callback) {
    // Similar to Futures.addCallback except doesn't double-wrap
    class CallbackListener implements Runnable {
      @Override public void run() {
        try {
          callback.onSuccess(future.getUninterruptibly());
        } catch (RuntimeException | Error e) {
          propagateIfFatal(e);
          callback.onError(e);
        }
      }
    }
    (future = newFuture()).addListener(new CallbackListener(), DirectExecutor.INSTANCE);
  }

  @Override public final void doCancel() {
    ResultSetFuture maybeFuture = future;
    if (maybeFuture != null) maybeFuture.cancel(true);
  }

  @Override public final boolean doIsCanceled() {
    ResultSetFuture maybeFuture = future;
    return maybeFuture != null && maybeFuture.isCancelled();
  }
}
