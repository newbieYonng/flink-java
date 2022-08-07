package com.yonng.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class MapDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> source = env.socketTextStream("yonng02", 8888);

        SingleOutputStreamOperator<String> myMap = source.transform(
                "MyMap",
                TypeInformation.of(String.class),
                new MyStreamMap());

        //source.map();

        myMap.print();

        env.execute();
    }

    private static class MyStreamMap extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            String in = element.getValue();
            String upper = in.toUpperCase();
//            output.collect(new StreamRecord<>(upper));
            output.collect(element.replace(upper));
        }
    }
}
