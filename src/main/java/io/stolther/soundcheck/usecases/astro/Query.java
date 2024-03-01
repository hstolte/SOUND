package io.stolther.soundcheck.usecases.astro;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.LatencyLoggingSink;
import io.stolther.soundcheck.core.*;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.xml.crypto.KeySelector;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Query {
    public static void main(String[] args) throws Exception {

        ExperimentSettings settings = ExperimentSettings.newInstance(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        FlinkSerializerActivator.NOPROVENANCE.activate(env, settings)
                .register(AstroWindowTuple.class, new AstroWindowTuple.KryoSerializer())
                .register(AstroDataPoint.class, new AstroDataPoint.KryoSerializer())
                .register(DataSeries.class, new DataSeries.KryoSerializer());


        final SingleOutputStreamOperator<AstroDataPoint> sourceStream =
                env.addSource(new AstroDataPointFileSource(settings))
                        .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());


        DataStream<AstroDataPoint> filtered = sourceStream.filter(new FilterFunction<AstroDataPoint>() {
            @Override
            public boolean filter(AstroDataPoint astroDataPoint) throws Exception {
                return astroDataPoint.is_upper_lim == 0 && astroDataPoint.relative_error < 5.0;
            }
        });


        DataStream<Tuple2<AstroDataPoint, Double>> smoothed = filtered
                .keyBy(x -> x.sourceName)
                .flatMap(new Query.SmoothingFunction());

        DataStream<Tuple3<DataSeries, DataSeries, Long>> comparisonSteam = smoothed
                .keyBy(value -> value.f0.sourceName)
                .countWindow(10)
                .apply(new WindowFunction<Tuple2<AstroDataPoint, Double>, Tuple3<DataSeries, DataSeries, Long>, String, GlobalWindow>() {
                    @Override
                    public void apply(String s, GlobalWindow globalWindow, Iterable<Tuple2<AstroDataPoint, Double>> iterable, Collector<Tuple3<DataSeries, DataSeries, Long>> collector) throws Exception {
                        ArrayList<Double> sList = new ArrayList<>();
                        ArrayList<Double> sigmaList = new ArrayList<>();
                        ArrayList<Double> oList = new ArrayList<>();

                        long maxStimulus = 0;

                        long t = 0;


                        for (Tuple2<AstroDataPoint, Double> point : iterable) {
                            sList.add(point.f1);
                            oList.add(point.f0.flux);
                            sigmaList.add(point.f0.sigma);
                            maxStimulus = Math.max(maxStimulus, point.f0.stimulus);

                            t = point.f0.time;
                        }


                        DataSeries smoothed = new DataSeries(t, sList.stream().mapToDouble(Double::doubleValue).toArray(), null, s);
                        DataSeries original = new DataSeries(t, oList.stream().mapToDouble(Double::doubleValue).toArray(), sigmaList.stream().mapToDouble(Double::doubleValue).toArray(), s);

                        collector.collect(Tuple3.of(original, smoothed, maxStimulus));
                    }
                });


        DataStream<Tuple2<DataSeries, Long>> resultStream = comparisonSteam
                .keyBy(value -> value.f0.key)
                .map(new MapFunction<Tuple3<DataSeries, DataSeries, Long>, Tuple2<DataSeries, Long>>() {


                    @Override
                    public Tuple2<DataSeries, Long> map(Tuple3<DataSeries, DataSeries, Long> t) throws Exception {
                        RealVector vector1 = new ArrayRealVector(t.f0.v);
                        RealVector vector2 = new ArrayRealVector(t.f1.v);

                        RealVector resultVector = vector1.subtract(vector2);
                        double[] resultArray = resultVector.toArray();

                        DataSeries result = new DataSeries(t.f0.t, resultArray, null, t.f0.key);
                        return Tuple2.of(result, t.f2);
                    }
                });


        resultStream.addSink(LatencyLoggingSink.newInstance("SINK", t -> t.f1, settings))
                .setParallelism(settings.parallelism());


        env.execute(QuerySoundNoOpSink.class.getSimpleName());

    }


    public static class SmoothingFunction extends RichFlatMapFunction<AstroDataPoint, Tuple2<AstroDataPoint, Double>> {
        private LinkedList<Double> window;
        private final int windowSize = 3;

        @Override
        public void open(Configuration parameters) {
            window = new LinkedList<>();
        }

        @Override
        public void flatMap(AstroDataPoint value, Collector<Tuple2<AstroDataPoint, Double>> out) {
            window.add(value.flux);
            if (window.size() > windowSize) {
                window.removeFirst();
            }

            if (window.size() == windowSize) {
                double sum = 0;
                for (double flux : window) {
                    sum += flux;
                }
                double smoothed = sum / windowSize;
                out.collect(new Tuple2<>(value, smoothed));
            }
        }
    }

}

