package com.yonng.demo;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ProcessingTimeTumblingWindowJoinDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //事实流
        //o001,c10,2000
        DataStreamSource<String> lines1 = env.socketTextStream("192.168.77.101", 8888);

        //维度流
        //c10,图书
        DataStreamSource<String> lines2 = env.socketTextStream("192.168.77.102", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Double>> orderTpStream = lines1.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, String>> categoryTpStream = lines2.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], fields[1]);
            }
        });
        
        orderTpStream.join(categoryTpStream)
                .where(tp -> tp.f1)
                .equalTo(tp -> tp.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new JoinFunction<Tuple3<String, String, Double>, Tuple2<String, String>, Tuple4<String, String, Double, String>>() {
                    @Override
                    public Tuple4<String, String, Double, String> join(Tuple3<String, String, Double> first, Tuple2<String, String> second) throws Exception {
                        return Tuple4.of(first.f0, first.f1, first.f2, second.f1);
                    }
                })
                .print();

        env.execute();
    }
}
