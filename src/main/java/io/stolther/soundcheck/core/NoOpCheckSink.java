package io.stolther.soundcheck.core;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;


public class NoOpCheckSink<T> implements SinkFunction<T> {
    private static final int LOG_NOOPSINK_ALIVE_EVERY_N_STEPS = 0;
    long counter = 0;

    @Override
    public void invoke(T value, Context context) throws Exception {
        SinkFunction.super.invoke(value, context);
        if (LOG_NOOPSINK_ALIVE_EVERY_N_STEPS > 0) {
            counter++;

            if (counter % LOG_NOOPSINK_ALIVE_EVERY_N_STEPS == 0) {
                System.out.println("Alive Noopsink " + value.toString());
            }
        }
    }
}