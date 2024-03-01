package io.stolther.soundcheck.core;

import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;

public class WindowValuesSink extends RichSinkFunction<VA2CheckResult> {
    private final ExperimentSettings settings;
    private transient PrintWriter pw;
    private final String name;
    private final int id;

    public WindowValuesSink(String name, int id, ExperimentSettings settings) {
        this.settings = settings;
        this.name = name;
        this.id = id;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        final int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
        try {
            pw =
                    new PrintWriter(new FileWriter(settings.windowDataFile(taskIndex, name, id)), settings.autoFlush());
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
        pw.println(VA2CheckResult.get_csv_header());

        super.open(parameters);
    }

    @Override
    public void invoke(VA2CheckResult tuple, Context context) {

        if (!tuple.key.equals("4FGL_J0035.8+61314FGL_J0035.8+6131")) {
            return;
        }


        // Example double array
        double[] array = tuple.get_values(id);

        // Convert double array to string
        String arrayString = Arrays.toString(array)
                .replace("[", "")   // Remove the opening bracket
                .replace("]", "")   // Remove the closing bracket
                .trim();            // Remove leading and trailing spaces

        pw.println(arrayString);

    }

    @Override
    public void close() throws Exception {
//        pw.print("--- OUTPUT END ---");
        pw.flush();
        pw.close();
        super.close();
    }
}

