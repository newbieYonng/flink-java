package state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ValueStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(10000);
        DataStreamSource<String> source = env.socketTextStream("yonng02", 8888);

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                String word = fields[0];
                if (word.startsWith("error"))
                    throw new RuntimeException("ERROR DATA!!!!!!!!");
                int cnt = Integer.parseInt(fields[1]);
                return Tuple2.of(word, cnt);
            }
        }).keyBy(tp -> tp.f0);

        keyedStream.map(new ValueStateReduceFunction()).print();

        env.execute();
    }

    private static class ValueStateReduceFunction extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private ValueState<Integer> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
            valueState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> in) throws Exception {
            Integer count = in.f1;
            Integer hisCount = valueState.value();
            if (hisCount == null)
                hisCount = 0;
            hisCount += count;
            valueState.update(hisCount);
            in.f1 = hisCount;
            return in;
        }
    }

}
