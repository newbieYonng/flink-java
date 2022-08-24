package com.yonng.demo;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class EventTimeTumblingWindowLeftOuterJoinDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.enableCheckpointing(30000);

        DataStreamSource<String> leftSource = env.socketTextStream("192.168.77.101", 8888);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> leftTpStream = leftSource.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                long ts = Long.parseLong(fields[0]);
                String cid = fields[1];
                int money = Integer.parseInt(fields[2]);
                return Tuple3.of(ts, cid, money);
            }
        }).setParallelism(2);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> leftStreamWithWaterMark = leftTpStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<Long, String, Integer>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<Long, String, Integer>>() {
                    @Override
                    public long extractTimestamp(Tuple3<Long, String, Integer> element, long recordTimestamp) {
                        return element.f0;
                    }
                }));

        DataStreamSource<String> rightSource = env.socketTextStream("192.168.77.102", 8888);

        SingleOutputStreamOperator<Tuple3<Long, String, String>> rightTpStream = rightSource.map(new MapFunction<String, Tuple3<Long, String, String>>() {
            @Override
            public Tuple3<Long, String, String> map(String line) throws Exception {
                String[] fields = line.split(",");
                long ts = Long.parseLong(fields[0]);
                String cid = fields[1];
                return Tuple3.of(ts, cid, fields[2]);
            }
        }).setParallelism(2);

        SingleOutputStreamOperator<Tuple3<Long, String, String>> rightStreamWithWaterMark = rightTpStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<Long, String, String>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<Long, String, String>>() {
                    @Override
                    public long extractTimestamp(Tuple3<Long, String, String> element, long recordTimestamp) {
                        return element.f0;
                    }
                }));

        leftStreamWithWaterMark.coGroup(rightStreamWithWaterMark)
                .where(tp -> tp.f1)
                .equalTo(tp -> tp.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<Tuple3<Long, String, Integer>, Tuple3<Long, String, String>, Tuple5<Long, String, Integer, Long, String>>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<Long, String, Integer>> first, Iterable<Tuple3<Long, String, String>> second, Collector<Tuple5<Long, String, Integer, Long, String>> out) throws Exception {
                        for (Tuple3<Long, String, Integer> val1 : first) {
                            boolean isJoined = false;
                            for (Tuple3<Long, String, String> val2 : second) {
                                isJoined = true;
                                out.collect(Tuple5.of(val1.f0, val1.f1, val1.f2, val2.f0, val2.f2));
                            }
                            if (!isJoined)
                                out.collect(Tuple5.of(val1.f0, val1.f1, val1.f2, null, null));
                        }
                    }
                }).print();

        env.execute();

    }
}
