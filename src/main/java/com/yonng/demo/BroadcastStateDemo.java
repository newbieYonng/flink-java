package com.yonng.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastStateDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.enableCheckpointing(30000);

        //维度流
        DataStreamSource<String> broadcastSource = env.socketTextStream("192.168.77.101", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = broadcastSource.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });

        MapStateDescriptor<String, String> broadcastState = new MapStateDescriptor<>("broadcast", String.class, String.class);
        BroadcastStream<Tuple3<String, String, String>> broadcastStream = tpStream.broadcast(broadcastState);

        //事实流
        DataStreamSource<String> source = env.socketTextStream("192.168.77.102", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> orderTpStream = source.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], Integer.parseInt(fields[2]));
            }
        });

        orderTpStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<Tuple3<String, String, Integer>, Tuple3<String, String, String>, Tuple4<String, String, Integer, String>>() {
                    @Override
                    public void processElement(Tuple3<String, String, Integer> in, ReadOnlyContext ctx, Collector<Tuple4<String, String, Integer, String>> out) throws Exception {
                        String id = in.f1;
                        ReadOnlyBroadcastState<String, String> broadcastState1 = ctx.getBroadcastState(broadcastState);
                        String name = broadcastState1.get(id);
                        out.collect(Tuple4.of(in.f0, in.f1, in.f2, name));
                    }

                    @Override
                    public void processBroadcastElement(Tuple3<String, String, String> in, Context ctx, Collector<Tuple4<String, String, Integer, String>> out) throws Exception {
                        String id = in.f0;
                        String name = in.f1;
                        String type = in.f2;
                        BroadcastState<String, String> broadcastState1 = ctx.getBroadcastState(broadcastState);
                        if ("delete".equals(type))
                            broadcastState1.remove(id);
                        else
                            broadcastState1.put(id, name);
                    }
                }).print();

        env.execute();

    }
}
