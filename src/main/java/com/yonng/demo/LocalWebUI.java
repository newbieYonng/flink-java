package com.yonng.demo;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class LocalWebUI {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        int parallelism = env.getParallelism();
        System.out.println("执行环境的并行度：" + parallelism);

        DataStreamSource<String> lines = env.socketTextStream("yonng02", 8888);
        int parallelism1 = lines.getParallelism();
        System.out.println("socketTextStream创建的DataStreamSource的并行度：" + parallelism1);

        SingleOutputStreamOperator<String> uppered = lines.map(line -> line.toUpperCase());
        int parallelism2 = uppered.getParallelism();
        System.out.println("调用完map方法得到的DataStream的并行度：" + parallelism2);

        DataStreamSink<String> print = uppered.print();
        int parallelism3 = print.getTransformation().getParallelism();
        System.out.println("调用完print方法得到的DataStreamSink的并行度：" + parallelism3);

        env.execute();
    }
}
