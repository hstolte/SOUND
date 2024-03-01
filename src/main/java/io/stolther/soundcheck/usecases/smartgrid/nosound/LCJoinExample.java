package io.stolther.soundcheck.usecases.smartgrid.nosound;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

public class LCJoinExample {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create two example streams
        DataStream<Tuple2<Long, String>> stream1 = env.fromElements(
                Tuple2.of(1L, "Stream1_Data1"),
                Tuple2.of(3L, "Stream1_Data2"),
                Tuple2.of(5L, "Stream1_Data3")
        );

        DataStream<Tuple2<Long, String>> stream2 = env.fromElements(
                Tuple2.of(2L, "Stream2_Data1"),
                Tuple2.of(4L, "Stream2_Data2"),
                Tuple2.of(6L, "Stream2_Data3")
        );

        // Assign watermarks based on the event timestamp
        DataStream<Tuple2<Long, String>> stream1WithWatermarks = stream1.assignTimestampsAndWatermarks(new TimestampExtractor());
        DataStream<Tuple2<Long, String>> stream2WithWatermarks = stream2.assignTimestampsAndWatermarks(new TimestampExtractor());

        // Merge the two streams based on the pivot time
        DataStream<Tuple2<Long, Tuple2<String, String>>> mergedStream = stream1WithWatermarks
                .connect(stream2WithWatermarks)
                .keyBy(tuple -> tuple.f0, tuple -> tuple.f0)
                .process(new MergeFunction());

        // Print the merged stream
        mergedStream.print();

        // Execute the job
        env.execute("Stream Merge Example");
    }

    private static class TimestampExtractor implements AssignerWithPeriodicWatermarks<Tuple2<Long, String>> {
        private static final long MAX_DELAY = 5000; // Max out-of-order delay in milliseconds

        private long currentMaxTimestamp = Long.MIN_VALUE;

        @Override
        public long extractTimestamp(Tuple2<Long, String> element, long previousElementTimestamp) {
            long timestamp = element.f0;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            // The watermark is the current maximum timestamp minus the maximum delay
            return new Watermark(currentMaxTimestamp - MAX_DELAY);
        }
    }

    private static class MergeFunction extends KeyedCoProcessFunction<Long, Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, Tuple2<String, String>>> {

        @Override
        public void processElement1(Tuple2<Long, String> value, Context ctx, Collector<Tuple2<Long, Tuple2<String, String>>> out) throws Exception {
            // Emit the element from stream1
            out.collect(Tuple2.of(value.f0, Tuple2.of(value.f1, null)));
        }

        @Override
        public void processElement2(Tuple2<Long, String> value, Context ctx, Collector<Tuple2<Long, Tuple2<String, String>>> out) throws Exception {
            // Emit the element from stream2
            out.collect(Tuple2.of(value.f0, Tuple2.of(null, value.f1)));
        }
    }
}



//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.util.Collector;
//import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
//import org.apache.flink.util.OutputTag;
//import org.apache.flink.streaming.api.watermark.Watermark;
//
//public class LCJoinExample {
//
//    public static void main(String[] args) throws Exception {
//        // Set up the execution environment
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // Create two example streams
//        DataStream<Tuple2<Long, String>> stream1 = env.fromElements(
//                Tuple2.of(1L, "Stream1_Data1"),
//                Tuple2.of(3L, "Stream1_Data2"),
//                Tuple2.of(5L, "Stream1_Data3")
//        );
//
//        DataStream<Tuple2<Long, String>> stream2 = env.fromElements(
//                Tuple2.of(2L, "Stream2_Data1"),
//                Tuple2.of(4L, "Stream2_Data2"),
//                Tuple2.of(6L, "Stream2_Data3")
//        );
//
//        // Define an output tag for unmatched elements from stream1
//        OutputTag<Tuple2<Long, String>> unmatchedStream1Tag = new OutputTag<Tuple2<Long, String>>("unmatchedStream1") {
//        };
//
//        // Define an output tag for unmatched elements from stream2
//        OutputTag<Tuple2<Long, String>> unmatchedStream2Tag = new OutputTag<Tuple2<Long, String>>("unmatchedStream2") {
//        };
//
//        // Merge the two streams based on the pivot time
//        DataStream<Tuple2<Long, Tuple2<String, String>>> mergedStream = stream1
//                .connect(stream2)
//                .keyBy(tuple -> tuple.f0, tuple -> tuple.f0)
//                .process(new MergeFunction(unmatchedStream1Tag, unmatchedStream2Tag));
//
//        // Print the merged stream
//        mergedStream.print();
//
////        // Print unmatched elements from stream1
////        DataStream<Tuple2<Long, String>> unmatchedStream1 = mergedStream.getSideOutput(unmatchedStream1Tag);
////        unmatchedStream1.print();
////
////        // Print unmatched elements from stream2
////        DataStream<Tuple2<Long, String>> unmatchedStream2 = mergedStream.getSideOutput(unmatchedStream2Tag);
////        unmatchedStream2.print();
//
//        // Execute the job
//        env.execute("Stream Merge Example");
//    }
//
//    private static class MergeFunction extends KeyedCoProcessFunction<Long, Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, Tuple2<String, String>>> {
//        private final OutputTag<Tuple2<Long, String>> unmatchedStream1Tag;
//        private final OutputTag<Tuple2<Long, String>> unmatchedStream2Tag;
//
//        public MergeFunction(OutputTag<Tuple2<Long, String>> unmatchedStream1Tag, OutputTag<Tuple2<Long, String>> unmatchedStream2Tag) {
//            this.unmatchedStream1Tag = unmatchedStream1Tag;
//            this.unmatchedStream2Tag = unmatchedStream2Tag;
//        }
//
//        @Override
//        public void processElement1(Tuple2<Long, String> value, Context ctx, Collector<Tuple2<Long, Tuple2<String, String>>> out) throws Exception {
//            // Emit the element from stream1
//            out.collect(Tuple2.of(value.f0, Tuple2.of(value.f1, null)));
//        }
//
//        @Override
//        public void processElement2(Tuple2<Long, String> value, Context ctx, Collector<Tuple2<Long, Tuple2<String, String>>> out) throws Exception {
//            // Emit the element from stream2
//            out.collect(Tuple2.of(value.f0, Tuple2.of(null, value.f1)));
//        }
//
//        @Override
//        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Long, Tuple2<String, String>>> out) throws Exception {
//            // Do nothing
//        }
//    }
//}
// ---------------------------------------------------------------------------------

//
//import org.apache.flink.api.common.functions.FlatMapFunction;
//        import org.apache.flink.api.java.tuple.Tuple2;
//        import org.apache.flink.streaming.api.datastream.DataStream;
//        import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//        import org.apache.flink.util.Collector;
//
//public class LCJoinExample {
//
//    public static void main(String[] args) throws Exception {
//        // Set up the execution environment
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // Create two example streams
//        DataStream<Tuple2<Long, String>> stream1 = env.fromElements(
//                Tuple2.of(1L, "Stream1_Data1"),
//                Tuple2.of(3L, "Stream1_Data2"),
//                Tuple2.of(5L, "Stream1_Data3")
//        );
//
//        DataStream<Tuple2<Long, String>> stream2 = env.fromElements(
//                Tuple2.of(2L, "Stream2_Data1"),
//                Tuple2.of(4L, "Stream2_Data2"),
//                Tuple2.of(6L, "Stream2_Data3")
//        );
//
//        // Merge the two streams based on the pivot time
//        DataStream<Tuple2<Long, Tuple2<String, String>>> mergedStream = stream1
//                .coGroup(stream2)
//                .where(tuple -> tuple.f0)
//                .equalTo(tuple -> tuple.f0)
//                .window(org.apache.flink.streaming.api.windowing.time.Time.seconds(1))  // Adjust the window size as per your requirement
//                .apply((left, right) -> {
//                    String stream1Data = (left.iterator().hasNext()) ? left.iterator().next().f1 : null;
//                    String stream2Data = (right.iterator().hasNext()) ? right.iterator().next().f1 : null;
//                    return Tuple2.of(left.iterator().next().f0, Tuple2.of(stream1Data, stream2Data));
//                });
//
//        // Print the merged stream
//        mergedStream.print();
//
//        // Execute the job
//        env.execute("Stream Merge Example");
//    }
//}
