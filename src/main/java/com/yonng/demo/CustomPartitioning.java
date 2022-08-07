package com.yonng.demo;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class CustomPartitioning {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> source = env.socketTextStream("192.168.77.102", 8888);

        SingleOutputStreamOperator<Tuple2<String, String>> mapStream = source.map(new RichMapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                int subtask = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(value, subtask + "");
            }
        }).setParallelism(4);

        DataStream<Tuple2<String, String>> partitionCustom = mapStream.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                System.out.println("下游分区的数量为：" + numPartitions);
                int index;
                if (key.startsWith("a"))
                    index = 1;
                else if (key.startsWith("b"))
                    index = 2;
                else
                    index = 3;
                return index;
            }
        }, tp -> tp.f0);

        partitionCustom.addSink(new RichSinkFunction<Tuple2<String, String>>() {
            @Override
            public void invoke(Tuple2<String, String> value, Context context) throws Exception {
                System.out.println(value.f0 + " : " + value.f1 + " -> " + getRuntimeContext().getIndexOfThisSubtask());
            }
        });

        env.execute();
    }
}
