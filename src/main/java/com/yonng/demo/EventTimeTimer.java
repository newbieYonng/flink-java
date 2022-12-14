package com.yonng.demo;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class EventTimeTimer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("192.168.77.101", 8888);

        SingleOutputStreamOperator<String> watermarks = source.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        return Long.parseLong(element.split(",")[0]);
                    }
                }));

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = watermarks.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        }).keyBy(tp -> tp.f0);

        keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private transient ValueState<List<Tuple2<String, Integer>>> listValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<List<Tuple2<String, Integer>>> stateDescriptor = new ValueStateDescriptor<>("count-state", TypeInformation.of(new TypeHint<List<Tuple2<String, Integer>>>() {
                }));
                listValueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                List<Tuple2<String, Integer>> lst = listValueState.value();
                if (lst == null)
                    lst = new ArrayList<>();
                lst.add(value);
                listValueState.update(lst);

                long watermark = ctx.timerService().currentWatermark();
                long triggerTime = (watermark - watermark % 10000) + 10000;
                System.out.println(ctx.getCurrentKey() + "???????????????????????????" + System.currentTimeMillis() + ", ???????????????????????????" + triggerTime);
                //?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
                ctx.timerService().registerProcessingTimeTimer(triggerTime);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                System.out.println("??????????????????????????????" + timestamp);
                List<Tuple2<String, Integer>> lst = listValueState.value();
                for (Tuple2<String, Integer> tuple2 : lst) {
                    out.collect(tuple2);
                }
                lst.clear();
            }
        }).print();

        env.execute();
    }
}
