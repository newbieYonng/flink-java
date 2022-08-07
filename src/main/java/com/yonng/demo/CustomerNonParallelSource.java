package com.yonng.demo;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class CustomerNonParallelSource {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<Integer> source = env.addSource(new NonParallelSourceFunc());
        System.out.println("自定义NonParallelSourceFunc得到的DataStream的并行度为：" + source.getParallelism());

        source.print();

        env.execute();
    }

    private static class NonParallelSourceFunc implements SourceFunction<Integer> {

        @Override
        public void run(SourceContext ctx) throws Exception {
            System.out.println("Run方法被调用了~~~~~");
            for (int i = 0; i < 100; i++) {
                //Source产生的数据使用SourceContext将数据输出
                ctx.collect(i);
            }
        }

        @Override
        public void cancel() {
            System.out.println("Cancel方法被调用了~~~~~");
        }
    }
}
