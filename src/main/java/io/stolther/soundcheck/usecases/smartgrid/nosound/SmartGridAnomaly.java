package io.stolther.soundcheck.usecases.smartgrid.nosound;


import io.palyvos.provenance.usecases.smartgrid.*;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.LatencyLoggingSink;
import io.stolther.soundcheck.core.DataPoint;
import io.stolther.soundcheck.core.DataSeries;
import io.stolther.soundcheck.core.NoOpCheckSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class SmartGridAnomaly {

    public static final int ANOMALY_INTERVAL_MINUTES = 1;
    public static final Time ANOMALY_HOUSEHOLD_AGGR_WINDOW_SIZE = Time.minutes(
            ANOMALY_INTERVAL_MINUTES);
    public static final Time ANOMALY_PLUG_AGGR_WINDOW_SIZE = Time.seconds(60);
    public static final Time ANOMALY_JOIN_WINDOW_SIZE = Time.seconds(15);
    public static final long ANOMALY_LIMIT = 5;

    public static final String GLOBAL_KEY = "globalKey";

    public static void main(String[] args) throws Exception {

        ExperimentSettings settings = ExperimentSettings.newInstance(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        FlinkSerializerActivator.NOPROVENANCE.activate(env, settings)
                .register(SmartGridTuple.class, new SmartGridTuple.KryoSerializer())
                .register(HouseholdUsageTuple.class, new HouseholdUsageTuple.KryoSerializer())
                .register(PlugUsageTuple.class, new PlugUsageTuple.KryoSerializer())
                .register(AnomalyResultTuple.class, new AnomalyResultTuple.KryoSerializer());

        final SingleOutputStreamOperator<SmartGridTuple> sourceStream =
                env.addSource(new SmartGridFileSource(settings))
                        .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());

        final SingleOutputStreamOperator<SmartGridTuple> loadStream = sourceStream.filter(t -> t.isLoad);
        final SingleOutputStreamOperator<SmartGridTuple> workStream = sourceStream.filter(t -> !t.isLoad);


        // ---- SGF-3 ------ //
        workStream.keyBy(new KeySelector<SmartGridTuple, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> getKey(SmartGridTuple t) throws Exception {
                        return Tuple3.of(t.houseId, t.householdId, t.plugId);
                    }
                })
                .window(TumblingEventTimeWindows.of(ANOMALY_JOIN_WINDOW_SIZE))
                .process(new ProcessWindowFunction<SmartGridTuple, DataSeries, Tuple3<Integer, Integer, Integer>, TimeWindow>() {
                    @Override
                    public void process(Tuple3<Integer, Integer, Integer> key, Context context, Iterable<SmartGridTuple> events, Collector<DataSeries> out) {
                        ArrayList<Double> valueList = new ArrayList<>();
                        events.forEach(tuple -> valueList.add(tuple.value));
                        double[] valueArray = valueList.stream().mapToDouble(Double::doubleValue).toArray();
                        long windowEnd = context.window().getEnd();
                        out.collect(new DataSeries(windowEnd, valueArray));
                    }
                })
                .addSink(new NoOpCheckSink<>())
                .setParallelism(settings.sinkParallelism());

        // ---- SGF-3 ------ //

        loadStream
                .map((MapFunction<SmartGridTuple, DataPoint>) smartGridTuple -> new DataPoint(smartGridTuple.measurementId, smartGridTuple.value))
                .addSink(new NoOpCheckSink<>())
                .setParallelism(settings.sinkParallelism());

        // ---- SGF-3 ------ //

        final WindowedStream<SmartGridTuple, Tuple3<Integer, Integer, Integer>, TimeWindow> plugStream =
                loadStream.filter(plugIntervalFilter())
                        .keyBy(new KeySelector<SmartGridTuple, Tuple3<Integer, Integer, Integer>>() {
                            @Override
                            public Tuple3<Integer, Integer, Integer> getKey(SmartGridTuple t) throws Exception {
                                return Tuple3.of(t.houseId, t.householdId, t.plugId);
                            }
                        })
                        .window(TumblingEventTimeWindows.of(ANOMALY_PLUG_AGGR_WINDOW_SIZE));


        final SingleOutputStreamOperator<PlugUsageTuple> intervalEnds = plugStream
                .aggregate(new AveragePlugUsageFunction()); // Handle double measurements with same ts

        final DataStream<PlugUsageTuple> cleanedIntervalEnds =
                intervalEnds.filter(intervalEndsFilter());


        final WindowedStream<SmartGridTuple, Tuple2<Integer, Integer>, TimeWindow> householdstream = loadStream
                .keyBy(new KeySelector<SmartGridTuple, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> getKey(SmartGridTuple t) throws Exception {
                        return Tuple2.of(t.houseId, t.householdId);
                    }
                })
                .window(TumblingEventTimeWindows.of(ANOMALY_HOUSEHOLD_AGGR_WINDOW_SIZE));


        // ------  constraint SGF-8 (plug count > household count in time window)  --------
        // todo update baseline with this

        DataStream<DataSeries> uniquePlugMeasurementsStream = loadStream
                .keyBy(event -> "globalKey")
                .timeWindow(ANOMALY_HOUSEHOLD_AGGR_WINDOW_SIZE)
                .process(new ProcessWindowFunction<SmartGridTuple, DataSeries, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<SmartGridTuple> events, Collector<DataSeries> out) {
                        HashMap<Tuple3<Integer, Integer, Integer>, Integer> uniqueTuples = new HashMap<>();
                        for (SmartGridTuple event : events) {
                            uniqueTuples.put(Tuple3.of(event.houseId, event.householdId, event.plugId), event.measurementId);
                        }
                        double[] measurementIds = uniqueTuples.values().stream().mapToDouble(i -> i).toArray();
                        long windowEnd = context.window().getEnd();
                        out.collect(new DataSeries(windowEnd, measurementIds));
                    }
                });

        DataStream<DataSeries> uniqueHouseholdMeasurementsStream = loadStream
                .keyBy(event -> "globalKey")
                .timeWindow(ANOMALY_HOUSEHOLD_AGGR_WINDOW_SIZE)
                .process(new ProcessWindowFunction<SmartGridTuple, DataSeries, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<SmartGridTuple> events, Collector<DataSeries> out) {
                        HashMap<Tuple2<Integer, Integer>, Integer> uniqueTuples = new HashMap<>();
                        for (SmartGridTuple event : events) {
                            uniqueTuples.put(Tuple2.of(event.houseId, event.householdId), event.measurementId);
                        }
                        double[] measurementIds = uniqueTuples.values().stream().mapToDouble(i -> i).toArray();
                        long windowEnd = context.window().getEnd();
                        out.collect(new DataSeries(windowEnd, measurementIds));
                    }
                });

        uniquePlugMeasurementsStream
                .join(uniqueHouseholdMeasurementsStream)
                .where(s -> s.t)
                .equalTo(s -> s.t)
                .window(TumblingEventTimeWindows.of(ANOMALY_HOUSEHOLD_AGGR_WINDOW_SIZE))
                .apply(new JoinFunction<DataSeries, DataSeries, Tuple2<DataSeries, DataSeries>>() {
                    @Override
                    public Tuple2<DataSeries, DataSeries> join(DataSeries dataSeries, DataSeries dataSeries2) throws Exception {
                        return Tuple2.of(dataSeries, dataSeries2);
                    }
                })
                .addSink(new NoOpCheckSink<>())

                .setParallelism(settings.sinkParallelism());

        // ------  end of constraint SGF-8 (plug count > household count in time window)  --------

        final SingleOutputStreamOperator<HouseholdUsageTuple> householdUsageStream = householdstream.aggregate(new AverageHouseholdUsageFunction())
                .setParallelism(settings.parallelism());


        // ------  start of constraint SGF-7   --------

        householdUsageStream
                .keyBy(new KeySelector<HouseholdUsageTuple, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> getKey(HouseholdUsageTuple t) throws Exception {
                        return Tuple2.of(t.houseId, t.householdId);
                    }
                })
                .timeWindow(ANOMALY_JOIN_WINDOW_SIZE)
                .process(new ProcessWindowFunction<HouseholdUsageTuple, DataSeries, Tuple2<Integer, Integer>, TimeWindow>() {
                    @Override
                    public void process(Tuple2<Integer, Integer> key, Context context, Iterable<HouseholdUsageTuple> events, Collector<DataSeries> out) {
                        ArrayList<Double> usageList = new ArrayList<>();
                        events.forEach(householdUsageTuple -> usageList.add(householdUsageTuple.usage));
                        double[] usageArray = usageList.stream().mapToDouble(Double::doubleValue).toArray();
                        long windowEnd = context.window().getEnd();
                        out.collect(new DataSeries(windowEnd, usageArray));
                    }
                })
                .addSink(new NoOpCheckSink<>())
                .setParallelism(settings.sinkParallelism());

        // ------  end of constraint SGF-7   --------

        final DataStream<AnomalyResultTuple> outputStream = householdUsageStream
                .join(cleanedIntervalEnds)
                .where(new KeySelector<HouseholdUsageTuple, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> getKey(HouseholdUsageTuple t) throws Exception {
                        return Tuple2.of(t.houseId, t.householdId);
                    }
                })
                .equalTo(new KeySelector<PlugUsageTuple, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> getKey(PlugUsageTuple t) throws Exception {
                        return Tuple2.of(t.houseId, t.householdId);
                    }
                }, new TypeHint<Tuple2<Integer, Integer>>() {
                }.getTypeInfo())
                .window(TumblingEventTimeWindows.of(ANOMALY_JOIN_WINDOW_SIZE))
                .with(new NewAnomalyJoinFunction())
                .setParallelism(settings.parallelism())
                .filter(t -> t.difference > ANOMALY_LIMIT)
                .setParallelism(settings.parallelism());

        outputStream.addSink(LatencyLoggingSink.newInstance("SINK", t -> t.stimulus, settings))
                .setParallelism(settings.parallelism());


        outputStream.map((MapFunction<AnomalyResultTuple, DataPoint>) tuple ->
                        new DataPoint(tuple.stimulus, tuple.difference))
                .addSink(new NoOpCheckSink<>())
                .setParallelism(settings.sinkParallelism());


        env.execute(io.stolther.soundcheck.usecases.smartgrid.sound.SmartGridAnomaly.class.getSimpleName());

    }

    public static FilterFunction<PlugUsageTuple> intervalEndsFilter() {
        return t -> t.usage > 0.5;
    }

    public static FilterFunction<SmartGridTuple> plugIntervalFilter() {
        return t ->
                TimeUnit.MILLISECONDS.toSeconds(t.timestamp) % TimeUnit.MINUTES.toSeconds(
                        ANOMALY_INTERVAL_MINUTES) == 0;
    }

//    public static <T, K> ProcessWindowFunction<T, DataSeries, K, TimeWindow> windowToDataSeries(Function<T, Double> extractor) {
//        return new ProcessWindowFunction<T, DataSeries, K, TimeWindow>() {
//            @Override
//            public void process(K key, Context context, Iterable<T> events, Collector<DataSeries> out) {
//                ArrayList<Double> list = new ArrayList<>();
//                events.forEach(event -> list.add(extractor.apply(event)));
//                double[] array = list.stream().mapToDouble(Double::doubleValue).toArray();
//                long windowEnd = context.window().getEnd();
//                out.collect(new DataSeries(windowEnd, array));
//            }
//        };
//    }


}
