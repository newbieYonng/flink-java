package com.yonng.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 *  将同一个用户的行为用KeyedState中的ListState保存起来
 *  ListState对key的每一个数据设置TTL
 */
public class ListStateTTLDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        DataStreamSource<String> source = env.socketTextStream("192.168.77.101", 8888);

        KeyedStream<Tuple2<String, String>, String> keyedStream = source.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], fields[1]);
            }
        }).keyBy(tp -> tp.f0);

        keyedStream.process(new IncomeHourlySumFunction()).print();

        env.execute();
    }

    public static class IncomeHourlySumFunction extends KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, String>> {

        private transient ListState<String> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<String>("hourly-state", String.class);
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(10)).build();
            stateDescriptor.enableTimeToLive(ttlConfig);
            listState = getRuntimeContext().getListState(stateDescriptor);
        }

        @Override
        public void processElement(Tuple2<String, String> in, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
            String curAction = in.f1;
            listState.add(curAction);
            for (String s : listState.get())
                out.collect(Tuple2.of(ctx.getCurrentKey(), s));
        }
    }

}
