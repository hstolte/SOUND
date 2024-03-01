package io.stolther.soundflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


class Event {
    long t;
    String v;

    public Event(long t, String v) {
        this.t = t;
        this.v = v;
    }

    @Override
    public String toString() {
        return "{" + v + '@' + t +'}';
    }
}

public class CountSlideWindowTest {

    @Test
    public void slide() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<Event> source1 = env.fromElements(
                new Event(1, "1A"),
                new Event(2, "1B"),
                new Event(3, "1C"),
                new Event(4, "1D"),
                new Event(5, "1E"));
        DataStreamSource<Event> source2 = env.fromElements(
                new Event(1, "2A"),
                new Event(2, "2B"),
                new Event(3, "2C"),
                new Event(4, "2D"),
                new Event(5, "2E"));

//        MapFunction<String, String> map_fn =

//        DataStream<String> stream1 = source1.countWindowAll(3, 1).reduce((ReduceFunction<String>) (s1, s2) -> s1+s2);
        DataStream<Event> stream2 = source2.countWindowAll(3, 1).reduce((event, t1) -> event);

//        source1.print();
        source2.print();
//        stream1.print();
        stream2.print();

        env.execute("TestJob");


        Assert.fail();


    }
}
