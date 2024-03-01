package io.stolther.soundcheck.core;

import io.palyvos.provenance.ananke.functions.noop.NoopSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class NoOpCheckResultSink implements SinkFunction<CheckResult> {

//    long counter = 0;

    @Override
    public void invoke(CheckResult value, Context context) throws Exception {
        SinkFunction.super.invoke(value, context);

//        counter++;
//
//        if (counter % 10000 == 0) {
//            System.out.println("Alive Noopsink " + value.toString());
//        }
    }
}
