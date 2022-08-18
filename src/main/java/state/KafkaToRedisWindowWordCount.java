package state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class KafkaToRedisWindowWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        //任务cancel保留外部存储checkpoint
        //如果不设置该属性DELETE_ON_CANCELLATION（job被cancel后，会删除外部的checkpoint数据）
        //一定要加上这是属性RETAIN_ON_CANCELLATION(job被cancel后，保留外部的checkpoint数据)
        //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置statebackend(设置状态存储的后端),默认JobManager内存中
        //env.setStateBackend(new FsStateBackend("hdfs://192.168.77.101:8020/data/checkpoint"));

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("yonng01:9092,yonng02:9092,yonng03:9092")
                .setTopics("wc")
                .setGroupId("wc-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                //取消kafka管理偏移量，不将偏移量写入到kafka特殊的topic中，让flink来管理偏移量
                .setProperty("enable.auto.commit", "false")
                .build();

        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source");

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(tp -> tp.f0);

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        windowedStream.reduce(new MyReduceFunction(), new HisSumWindowFunction()).print();

        env.execute();
    }

    public static class MyReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
            value1.f1 += value2.f1;
            return value1;
        }
    }

    public static class HisSumWindowFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

        private transient ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
            countState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
            Tuple2<String, Integer> tp2 = elements.iterator().next();
            Integer his = countState.value();
            if (his == null)
                his = 0;
            his += tp2.f1;
            countState.update(his);
            tp2.f1 = his;
            out.collect(tp2);
        }
    }
}
