package com.yonng.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 *  KeyedState ValueState， KEY是keyBy的字段
 *  CopyOnWriteMap<KEY, VALUE> 对KEY设置的TTL
 */
public class ValueStateTTLDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("192.168.77.101", 8888);

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        }).keyBy(tp -> tp.f0);

        keyedStream.process(new IncomeHourlySumFunction()).print();

        env.execute();
    }

    public static class IncomeHourlySumFunction extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private transient ValueState<Integer> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<Integer>("hourly-state", Integer.class);
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(20)).build();
            stateDescriptor.enableTimeToLive(ttlConfig);
            valueState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(Tuple2<String, Integer> in, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            Integer current = in.f1;
            Integer his = valueState.value();
            if (his == null)
                his = 0;
            current += his;
            valueState.update(current);
            in.f1 = current;
            out.collect(in);
        }
    }
}
