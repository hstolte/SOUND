package io.stolther.soundcheck.usecases.smartgrid.nosound;


import io.palyvos.provenance.usecases.smartgrid.*;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.LatencyLoggingSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Int;

import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

public class SmartGridMonitorA {

  public static final Time SGM_WINDOW_SIZE = Time.seconds(15);
  public static final int ANOMALY_INTERVAL_MINUTES = 1;
  public static final Time ANOMALY_HOUSEHOLD_AGGR_WINDOW_SIZE = Time.minutes(
      ANOMALY_INTERVAL_MINUTES);
  public static final Time ANOMALY_PLUG_AGGR_WINDOW_SIZE = Time.seconds(60);
  public static final Time ANOMALY_JOIN_WINDOW_SIZE = Time.seconds(15);
  public static final long ANOMALY_LIMIT = 5;

  public static void main(String[] args) throws Exception {

    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().enableObjectReuse();

    FlinkSerializerActivator.NOPROVENANCE.activate(env, settings)
        .register(SmartGridTuple.class, new SmartGridTuple.KryoSerializer())
        .register(HouseholdUsageTuple.class, new HouseholdUsageTuple.KryoSerializer())
        .register(PlugUsageTuple.class, new PlugUsageTuple.KryoSerializer())
        .register(AnomalyResultTuple.class, new AnomalyResultTuple.KryoSerializer());



    final SingleOutputStreamOperator<SmartGridTuple> fullInputStream =
        env.addSource(new SmartGridFileSource(settings))
            .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());

    // global window, filter load, sum load

    fullInputStream
            .filter(new LoadFilter())
            .windowAll(TumblingEventTimeWindows.of(SGM_WINDOW_SIZE))
            .reduce(new SumValueReduceFunction())
            .print();


    // group by plug id, filter load, sum load, filter by sum > 0, count tuples in window

      DataStream<SmartGridTuple> onlyActivePlugsInWindow = fullInputStream
              .filter(new LoadFilter())
              .keyBy(new PlugKeySelector())
              .window(TumblingEventTimeWindows.of(SGM_WINDOW_SIZE))
              .reduce(new SumValueReduceFunction())
              .filter(new PositiveNonZeroValueFilter());





    final SingleOutputStreamOperator<Object> perHouseholdWindowedStream = fullInputStream
            .keyBy(new HouseholdKeySelector())
            .window(TumblingEventTimeWindows.of(SGM_WINDOW_SIZE))
            .process(new ProcessWindowFunction<SmartGridTuple, Object, Tuple2<Integer, Integer>, TimeWindow>() {

              @Override
              public void process(Tuple2<Integer, Integer> key,
                                  ProcessWindowFunction<SmartGridTuple, Object, Tuple2<Integer, Integer>, TimeWindow>.Context context,
                                  Iterable<SmartGridTuple> elements,
                                  Collector<Object> out) throws Exception {
                long count = StreamSupport.stream(elements.spliterator(), false).count();
                out.collect(new Tuple2<>(context.window(), count));
              }
            })
            .setParallelism(settings.parallelism());

//    perHouseholdWindowedStream.print();


    // Filter the tuples where isLoad is true
    DataStream<SmartGridTuple> filteredStream = fullInputStream
            .filter(new FilterFunction<SmartGridTuple>() {
              @Override
              public boolean filter(SmartGridTuple tuple) throws Exception {
                return tuple.isLoad;
              }
            });

// Map each tuple to a Tuple4 of (householdId, plugId, load, timestamp)
    DataStream<Tuple4<Integer, Integer, Double, Long>> mappedStream = filteredStream
            .map(new MapFunction<SmartGridTuple, Tuple4<Integer, Integer, Double, Long>>() {
              @Override
              public Tuple4<Integer, Integer, Double, Long> map(SmartGridTuple tuple) throws Exception {
                return new Tuple4<>(tuple.householdId, tuple.plugId, tuple.value, tuple.timestamp);
              }
            });

// Apply a sliding window of 5 seconds
//    DataStream<Tuple4<Integer, Integer, Double, Long>> windowedStream = mappedStream
//            .windowAll(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(5)));
//            .apply(new WindowFunction<Tuple4<Integer, Integer, Double, Long>, Tuple4<Integer, Integer, Double, Long>, Tuple, TimeWindow>() {
//              @Override
//              public void apply(Tuple key,
//                                TimeWindow window,
//                                Iterable<Tuple4<Integer, Integer, Double, Long>> input,
//                                Collector<Tuple4<Integer, Integer, Double, Long>> out) throws Exception {
//                Set<Integer> uniqueHouseholds = new HashSet<>();
//                Set<Integer> uniquePlugs = new HashSet<>();
//                double totalLoad = 0.0;
//                long endOfWindow = window.getEnd();
//
//                for (Tuple4<Integer, Integer, Double, Long> record : input) {
//                  uniqueHouseholds.add(record.f0);
//                  uniquePlugs.add(record.f1);
//                  totalLoad += record.f2;
//                }
//
//                out.collect(new Tuple4<>(uniqueHouseholds.size(), uniquePlugs.size(), totalLoad, endOfWindow));
//              }
//            });

// Print the result
//    windowedStream.print();
    
    final SingleOutputStreamOperator<SmartGridTuple> loadStream = fullInputStream.filter(t -> t.isLoad);
    final SingleOutputStreamOperator<SmartGridTuple> workStream = fullInputStream.filter(t -> !t.isLoad);

//    loadStream
//              .keyBy((KeySelector<SmartGridTuple, Tuple2<Integer, Integer>>) t -> Tuple2.of(t.houseId, t.householdId))
//              .window(TumblingEventTimeWindows.of(SGM_WINDOW_SIZE))
//              .aggregate(new AverageHouseholdUsageFunction())
//              .setParallelism(settings.parallelism());
//
//
//    workStream
//            .keyBy((KeySelector<SmartGridTuple, Tuple3<Integer, Integer, Integer>>) t -> Tuple3.of(t.houseId, t.householdId, t.plugId))
//            .window(TumblingEventTimeWindows.of(SGM_WINDOW_SIZE))
//            .aggregate(new AverageHouseholdUsageFunction())
//            .setParallelism(settings.parallelism());


//    final SingleOutputStreamOperator<PlugUsageTuple> intervalEnds =
//        sourceStream.filter(plugIntervalFilter())
//            .keyBy(new KeySelector<SmartGridTuple, Tuple3<Integer, Integer, Integer>>() {
//              @Override
//              public Tuple3<Integer, Integer, Integer> getKey(SmartGridTuple t) throws Exception {
//                return Tuple3.of(t.houseId, t.householdId, t.plugId);
//              }
//            })
//            .window(TumblingEventTimeWindows.of(ANOMALY_PLUG_AGGR_WINDOW_SIZE))
//            .aggregate(new AveragePlugUsageFunction()); // Handle double measurements with same ts
//
//    final DataStream<PlugUsageTuple> cleanedIntervalEnds =
//        intervalEnds.filter(intervalEndsFilter());
//
//    sourceStream
//        .keyBy(new KeySelector<SmartGridTuple, Tuple2<Integer, Integer>>() {
//          @Override
//          public Tuple2<Integer, Integer> getKey(SmartGridTuple t) throws Exception {
//            return Tuple2.of(t.houseId, t.householdId);
//          }
//        })
//        .window(TumblingEventTimeWindows.of(ANOMALY_HOUSEHOLD_AGGR_WINDOW_SIZE))
//        .aggregate(new AverageHouseholdUsageFunction())
//        .setParallelism(settings.parallelism())
//        .join(cleanedIntervalEnds)
//        .where(new KeySelector<HouseholdUsageTuple, Tuple2<Integer, Integer>>() {
//          @Override
//          public Tuple2<Integer, Integer> getKey(HouseholdUsageTuple t) throws Exception {
//            return Tuple2.of(t.houseId, t.householdId);
//          }
//        })
//        .equalTo(new KeySelector<PlugUsageTuple, Tuple2<Integer, Integer>>() {
//          @Override
//          public Tuple2<Integer, Integer> getKey(PlugUsageTuple t) throws Exception {
//            return Tuple2.of(t.houseId, t.householdId);
//          }
//        }, new TypeHint<Tuple2<Integer, Integer>>() {
//        }.getTypeInfo())
//        .window(TumblingEventTimeWindows.of(ANOMALY_JOIN_WINDOW_SIZE))
//        .with(new NewAnomalyJoinFunction())
//        .setParallelism(settings.parallelism())
//        .filter(t -> t.difference > ANOMALY_LIMIT)
//        .setParallelism(settings.parallelism())
//        .addSink(LatencyLoggingSink.newInstance("SINK", t -> t.stimulus, settings))
//        .setParallelism(settings.parallelism());
//
    env.execute(SmartGridMonitorA.class.getSimpleName());

  }

  public static class HouseholdKeySelector implements KeySelector<SmartGridTuple, Tuple2<Integer, Integer>> {
      @Override
      public Tuple2<Integer, Integer> getKey(SmartGridTuple smartGridTuple) throws Exception {
          return smartGridTuple.householdKey();
      }
  }

    public static class PlugKeySelector implements KeySelector<SmartGridTuple, Tuple3<Integer, Integer, Integer>> {
        @Override
        public Tuple3<Integer, Integer, Integer> getKey(SmartGridTuple smartGridTuple) throws Exception {
            return smartGridTuple.plugKey();
        }
    }

    public static class LoadFilter implements FilterFunction<SmartGridTuple> {
        @Override
        public boolean filter(SmartGridTuple tuple) throws Exception {
            return tuple.isLoad;
        }
    }

    public static class WorkFilter implements FilterFunction<SmartGridTuple> {
        @Override
        public boolean filter(SmartGridTuple tuple) throws Exception {
            return !tuple.isLoad;
        }
    }

  public static FilterFunction<PlugUsageTuple> intervalEndsFilter() {
    return t -> t.usage > 0.5;
  }

  public static FilterFunction<SmartGridTuple> plugIntervalFilter() {
    return t ->
        TimeUnit.MILLISECONDS.toSeconds(t.timestamp) % TimeUnit.MINUTES.toSeconds(
            ANOMALY_INTERVAL_MINUTES) == 0;
  }



  public static class SumValueReduceFunction implements ReduceFunction<SmartGridTuple> {
          @Override
          public SmartGridTuple reduce(SmartGridTuple value1, SmartGridTuple value2) {
              return new SmartGridTuple(value1.measurementId,
                      Math.max(value1.timestamp, value2.timestamp),  // use the latest timestamp
                      value1.value + value2.value,  // sum the values
                      value1.isLoad,
                      value1.plugId,
                      value1.householdId,
                      value1.houseId,
                      Math.max(value1.stimulus, value2.stimulus));  // use the latest stimulus
          }
  };

    public static class PositiveNonZeroValueFilter implements FilterFunction<SmartGridTuple> {
        @Override
        public boolean filter(SmartGridTuple smartGridTuple) throws Exception {
            return smartGridTuple.value > 0;
        }
    }


}
