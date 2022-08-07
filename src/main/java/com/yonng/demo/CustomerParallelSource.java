package com.yonng.demo;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class CustomerParallelSource {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> nums = env.addSource(new ParallelSourceFunc());

        System.out.println("自定义ParallelSourceFunc得到的DataStream的并行度为：" + nums.getParallelism());

        nums.print();

        env.execute();

    }

    private static class ParallelSourceFunc extends RichParallelSourceFunction<String> {

        private boolean flag = true;

        public ParallelSourceFunc() {
            System.out.println("构造方法执行了！！！！！！！！！！！");
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println(indexOfThisSubtask + ": Open方法被调用***********");
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println(indexOfThisSubtask +" : Run方法被调用了￥￥￥￥￥￥￥￥");
            Random random = new Random();

            while (flag) {
                int i = random.nextInt(100);
                ctx.collect(indexOfThisSubtask + " --> " + i);
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println(indexOfThisSubtask + " : Cancel方法被调用了~~~~~");
            flag = false;
        }

        @Override
        public void close() throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println(indexOfThisSubtask + " : Close方法被调用@@@@@@@@@");
        }
    }
}
