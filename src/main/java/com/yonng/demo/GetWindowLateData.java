package com.yonng.demo;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class GetWindowLateData {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> source = env.socketTextStream("192.168.77.101", 8888);

        SingleOutputStreamOperator<String> watermarks = source.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        return Long.parseLong(element.split(",")[0]);
                    }
                }));

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> mapStream = watermarks.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String in) throws Exception {
                String[] fields = in.split(",");
                return Tuple3.of(Long.parseLong(fields[0]), fields[1], Integer.parseInt(fields[0]));
            }
        });

        KeyedStream<Tuple3<Long, String, Integer>, String> keyedStream = mapStream.keyBy(tp -> tp.f1);

        OutputTag<Tuple3<Long, String, Integer>> lateOutputTag = new OutputTag<Tuple3<Long, String, Integer>>("late-tag") {};

        WindowedStream<Tuple3<Long, String, Integer>, String, TimeWindow> windowedStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sideOutputLateData(lateOutputTag);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> res = windowedStream.sum(2);

        DataStream<Tuple3<Long, String, Integer>> lateDataStream = res.getSideOutput(lateOutputTag);

        lateDataStream.print("late ");
        res.print("normal ");

        env.execute();
    }
}
