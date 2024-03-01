package io.stolther.soundcheck.core;

import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class VA2CheckResultSink extends RichSinkFunction<VA2CheckResult> {
    private final ExperimentSettings settings;
    private transient PrintWriter pw;
    private final String name;

    public VA2CheckResultSink(String name, ExperimentSettings settings) {
        this.settings = settings;
        this.name = name;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        final int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
        try {
            pw =
                    new PrintWriter(new FileWriter(settings.checkResultFile(taskIndex, name)), settings.autoFlush());
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
        pw.println(VA2CheckResult.get_csv_header());

        super.open(parameters);
    }

    @Override
    public void invoke(VA2CheckResult tuple, Context context) {
        pw.println(tuple.to_csv());
    }

    @Override
    public void close() throws Exception {
//        pw.print("--- OUTPUT END ---");
        pw.flush();
        pw.close();
        super.close();
    }
}

