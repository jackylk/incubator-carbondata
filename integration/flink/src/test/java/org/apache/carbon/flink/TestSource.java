package org.apache.carbon.flink;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

final class TestSource implements SourceFunction<String> {

    static final AtomicInteger DATA_COUNT = new AtomicInteger(0);

    TestSource(final String value) {
        this.value = value;
    }

    private final String value;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        for (int index = 1; index < 500; index++) {
            Thread.sleep(new Random().nextInt(5));
            sourceContext.collectWithTimestamp(
                this.value.replace("test", "test" + DATA_COUNT.get()),
                System.currentTimeMillis()
            );
        }
    }

    @Override
    public void cancel() {
        // to do nothing.
    }

}
