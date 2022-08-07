package com.yonng.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceDemo {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> source = env.socketTextStream("yonng02", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] fileds = line.split(",");
                return Tuple2.of(fileds[0], Integer.parseInt(fileds[1]));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mapStream.keyBy(tp -> tp.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduced = keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tp1, Tuple2<String, Integer> tp2) throws Exception {
                //return Tuple2.of(tp1.f0, tp1.f1 + tp2.f1);
                tp1.f1 = tp1.f1 + tp2.f1;
                return tp1;
            }
        });

        //keyedStream.maxBy()
        //keyedStream.sum()
        //mapStream.union(mapStream).union(mapStream)
        keyedStream.union(mapStream);

        reduced.print();

        env.execute();
    }
}
