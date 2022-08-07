package com.yonng.demo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByDemo01 {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> source = env.socketTextStream("yonng02", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = source.map(line -> {
            String[] fileds = line.split(",");
            return Tuple2.of(fileds[0], Integer.parseInt(fileds[1]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        //KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndCount.keyBy(tp -> tp.f0);

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndCount.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });


        keyedStream.print();

        env.execute();
    }
}
