package com.yonng.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

public class MapStateTTLDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("192.168.77.101", 8888);

        KeyedStream<Tuple2<String, Double>, String> keyedStream = source.map(new MapFunction<String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Double.parseDouble(fields[1]));
            }
        }).keyBy(tp -> tp.f0);

        keyedStream.process(new IncomeHourlySumFunction()).print();

        env.execute();
    }

    public static class IncomeHourlySumFunction extends KeyedProcessFunction<String, Tuple2<String, Double>, Tuple3<String, String, Double>> {

        private transient MapState<String, Double> mapState;
        private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HHmm");

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Double> stateDescriptor = new MapStateDescriptor<>("hourly-state", String.class, Double.class);
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.minutes(1))
                    //.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    //.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            stateDescriptor.enableTimeToLive(ttlConfig);
            mapState = getRuntimeContext().getMapState(stateDescriptor);
        }

        @Override
        public void processElement(Tuple2<String, Double> in, Context ctx, Collector<Tuple3<String, String, Double>> out) throws Exception {
            Double current = in.f1;
            String currentTime = sdf.format(new Date(System.currentTimeMillis()));
            Double history = mapState.get(currentTime);
            if (history == null)
                history = 0.0;
            current += history;
            mapState.put(currentTime, current);
            /*Iterator<Map.Entry<String, Double>> iterator = mapState.entries().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Double> entry = iterator.next();
                out.collect(Tuple3.of(ctx.getCurrentKey(), entry.getKey(), entry.getValue()));
            }*/
            out.collect(Tuple3.of(ctx.getCurrentKey(), currentTime, current));
        }
    }
}
