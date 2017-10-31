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

import java.io.IOException;
import java.util.List;
import zipkin2.Call;
import zipkin2.Callback;

abstract class AggregateCall<I, O> extends Call.Base<O> {
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
