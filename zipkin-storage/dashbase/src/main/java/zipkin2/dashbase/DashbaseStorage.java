package zipkin2.dashbase;

import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;

public class DashbaseStorage extends StorageComponent {

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder extends StorageComponent.Builder {
        boolean strictTraceId = true;
        /** {@inheritDoc} */
        @Override public Builder strictTraceId(boolean strictTraceId) {
            this.strictTraceId = strictTraceId;
            return this;
        }

        @Override public DashbaseStorage build() {
            return new DashbaseStorage(this);
        }
    }

    DashbaseStorage(Builder builder) {

    }

    @Override
    public SpanStore spanStore() {
        return new DashbaseSpanStore();
    }

    @Override
    public SpanConsumer spanConsumer() {
        // return new DashbaseSpanConsumer();
        return null;
    }
}
