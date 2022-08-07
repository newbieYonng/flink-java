package com.yonng.demo;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectDemo {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> source1 = env.socketTextStream("yonng02", 9999);
        DataStreamSource<String> source2 = env.socketTextStream("yonng02", 9998);

        ConnectedStreams<String, String> connectedStreams = source1.connect(source2);

        SingleOutputStreamOperator<String> resStream = connectedStreams.map(new CoMapFunction<String, String, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value;
            }

            @Override
            public String map2(String value) throws Exception {
                return value.toUpperCase();
            }
        });

        resStream.print();

        env.execute();
    }
}
