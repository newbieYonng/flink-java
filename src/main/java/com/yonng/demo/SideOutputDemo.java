package com.yonng.demo;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> source = env.socketTextStream("192.168.77.101", 8888);

        //奇数标签
        OutputTag<Integer> oddTag = new OutputTag<Integer>("odd-tag") {};
        //偶数标签
        OutputTag<Integer> evenTag = new OutputTag<Integer>("even-tag") {};
        //字符串标签
        OutputTag<String> strTag = new OutputTag<String>("str-tag") {};

        SingleOutputStreamOperator<String> process = source.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                try {
                    int num = Integer.parseInt(value);
                    if (num % 2 == 0)
                        ctx.output(evenTag, num);
                    else
                        ctx.output(oddTag, num);
                } catch (NumberFormatException e) {
                    ctx.output(strTag, value);
                }

                out.collect(value);
            }
        });

        DataStream<Integer> oddSideOutput = process.getSideOutput(oddTag);
        oddSideOutput.print("odd ");

        DataStream<String> strSideOutput = process.getSideOutput(strTag);
        strSideOutput.print("str ");

        process.print("main ");

        env.execute();
    }
}
