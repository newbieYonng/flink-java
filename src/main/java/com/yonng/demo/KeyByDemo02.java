package com.yonng.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByDemo02 {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> source = env.socketTextStream("yonng02", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> mapStream = source.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String line) throws Exception {
                String[] fileds = line.split(",");
                return Tuple3.of(fileds[0], fileds[1], Integer.parseInt(fileds[2]));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT));

        KeyedStream<Tuple3<String, String, Integer>, String> keyedStream = mapStream.keyBy(tp -> tp.f0 + tp.f1);
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> sum = keyedStream.sum(2);
        sum.print();

        env.execute();
    }
}
