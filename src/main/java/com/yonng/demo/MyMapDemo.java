package com.yonng.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class MyMapDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> source = env.socketTextStream("yonng02", 9999);

        //计算逻辑--动态修改
        MapFunction<String, String> mapFunction = new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase();
            }
        };

        SingleOutputStreamOperator<String> mapStream = source.transform(
                "MyMap",
                TypeInformation.of(String.class),
                new MyStreamMap<>(mapFunction)
        );

        mapStream.print();

        env.execute();
    }

    private static class MyStreamMap<I, O> extends AbstractUdfStreamOperator<O, MapFunction<I, O>> implements OneInputStreamOperator<I, O> {

        public MyStreamMap(MapFunction<I, O> userFunction) {
            super(userFunction);
        }

        @Override
        public void processElement(StreamRecord<I> element) throws Exception {
            I in = element.getValue();
            O out = userFunction.map(in);
            output.collect(element.replace(out));
        }
    }
}
