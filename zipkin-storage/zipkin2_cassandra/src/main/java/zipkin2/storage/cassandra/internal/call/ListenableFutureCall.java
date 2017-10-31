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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import zipkin2.Call;
import zipkin2.Callback;

import static com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly;

public abstract class ListenableFutureCall<V> extends Call.Base<V> {
  volatile ListenableFuture<V> future;

  @Override protected final V doExecute() throws IOException {
    return Futures.getUnchecked(future = newFuture()); // safe as guava 10
  }

  @Override protected final void doEnqueue(Callback<V> callback) {
    // Similar to Futures.addCallback except doesn't double-wrap
    class CallbackListener implements Runnable {
      @Override public void run() {
        final V value;
        try {
          value = getUninterruptibly(future);
        } catch (ExecutionException e) {
          callback.onError(e.getCause());
          return;
        } catch (RuntimeException | Error e) {
          callback.onError(e);
          return;
        }
        callback.onSuccess(value);
      }
    }
    (future = newFuture()).addListener(new CallbackListener(), DirectExecutor.INSTANCE);
  }

  /** Same as {@code MoreExecutors.directExecutor()} except without a guava 18 dep */
  private enum DirectExecutor implements Executor {
    INSTANCE;

    @Override public void execute(Runnable command) {
      command.run();
    }

    @Override public String toString() {
      return "MoreExecutors.directExecutor()";
    }
  }

  /** Defers I/O until {@link #enqueue(Callback)} or {@link #execute()} are called. */
  protected abstract ListenableFuture<V> newFuture();

  @Override public final void doCancel() {
    ListenableFuture<V> maybeFuture = future;
    if (maybeFuture != null) maybeFuture.cancel(true);
  }

  @Override public final boolean doIsCanceled() {
    ListenableFuture<V> maybeFuture = future;
    return maybeFuture != null && maybeFuture.isCancelled();
  }
}
