package zipkin2.dashbase;

import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;

import java.util.List;

final public class DashbaseSpanStore implements SpanStore {
    @Override
    public Call<List<List<Span>>> getTraces(QueryRequest queryRequest) {
        return null;
    }

    @Override
    public Call<List<Span>> getTrace(String s) {
        return null;
    }

    @Override
    public Call<List<String>> getServiceNames() {
        return null;
    }

    @Override
    public Call<List<String>> getSpanNames(String s) {
        return null;
    }

    @Override
    public Call<List<DependencyLink>> getDependencies(long l, long l1) {
        return null;
    }
}
