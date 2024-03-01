package io.stolther.soundcheck.usecases.astro;

import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.LatencyLoggingSink;
import io.stolther.soundcheck.core.*;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class QuerySoundNoOpSink {

    public static SinkFunction<CheckResult> get_check_sink(String name, ExperimentSettings settings) {
        return new NoOpCheckResultSink();
    }

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

        // Additional sanity checks

        UnaryPointBasedCheck APP_1 = new UnaryPointBasedCheck() {
            @Override
            protected boolean constraint(double x) {
                return x < 1 && 0 < x;
            }
        };
        APP_1.init(settings);
        filtered
                .map((MapFunction<AstroDataPoint, DataPoint>) t -> new DataPoint(t.time, t.flux, t.sigma))
                .map(APP_1)
                .setParallelism(settings.parallelism())
                .addSink(get_check_sink("A-1", settings))
                .setParallelism(settings.parallelism());


        BinarySequenceCheck A3 = new BinarySequenceCheck() {
            @Override
            public boolean constraint(double[] x, double[] y) {
                if (x.length < 2 || y.length < 2) {
                    return true;
                }
                double corr = MathUtils.pearsonCorrelation(x, y);
                if (Double.isNaN(corr)) {
                    return true;
                } else {
                    return corr > 0.2;
                }
            }
        };
        A3.init(settings);

        DataStream<Tuple2<DataSeries, DataSeries>> twoSeriesStream = comparisonSteam.map(new MapFunction<Tuple3<DataSeries, DataSeries, Long>, Tuple2<DataSeries, DataSeries>>() {
            @Override
            public Tuple2<DataSeries, DataSeries> map(Tuple3<DataSeries, DataSeries, Long> t) throws Exception {
                return Tuple2.of(t.f0, t.f1);
            }
        });

        twoSeriesStream.map(A3)
                .setParallelism(settings.parallelism())
                .addSink(get_check_sink("A-3", settings));


        BinarySequenceCheck A4 = new BinarySequenceCheck() {
            @Override
            public boolean constraint(double[] x, double[] y) {
                double avgDeltaX = averageDelta(x);
                double avgDeltaY = averageDelta(y);
                return avgDeltaX < avgDeltaY;
            }

            public double averageDelta(double[] array) {
                if (array.length < 2) {
                    throw new IllegalArgumentException("Array should have at least two elements.");
                }
                double sumDelta = 0.0;
                for (int i = 1; i < array.length; i++) {
                    sumDelta += Math.abs(array[i] - array[i - 1]);
                }
                return sumDelta / (array.length - 1);
            }

        };
        A4.init(settings);
        twoSeriesStream.map(A4)
                .setParallelism(settings.parallelism())
                .addSink(get_check_sink("A-4", settings));


        UnarySetCheck APP_2 = new UnarySetCheck() {
            @Override
            public boolean constraint(double[] x) {
                return MathUtils.std(x) > 0;
            }
        };
        APP_2.init(settings);

        filtered
                .keyBy(new KeySelector<AstroDataPoint, String>() {
                    @Override
                    public String getKey(AstroDataPoint astroDataPoint) throws Exception {
                        return astroDataPoint.sourceName;
                    }
                })
                .countWindow(3)
                .process(new ProcessWindowFunction<AstroDataPoint, DataSeries, String, GlobalWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<AstroDataPoint> events, Collector<DataSeries> out) {
                        int size = 3;
                        double[] values = new double[size];
                        double[] sigmas = new double[size];
                        int i = 0;

                        for (AstroDataPoint event : events) {
                            values[i] = event.flux;
                            sigmas[i] = event.sigma;
                            i++;
                        }

                        long windowEnd = context.window().maxTimestamp();
                        out.collect(new DataSeries(windowEnd, values, sigmas, key));
                    }
                })
                .map(APP_2)
                .setParallelism(settings.parallelism())
                .addSink(get_check_sink("A-2", settings))
                .setParallelism(settings.parallelism());


        env.execute(QuerySoundNoOpSink.class.getSimpleName());

    }
}
