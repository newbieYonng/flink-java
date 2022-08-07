package state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyAtLeastOnceFileSourceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(20000);
        env.setParallelism(4);

        DataStreamSource<String> source1 = env.addSource(new MyAtLeastOnceFileSource("C:\\Users\\83460\\Desktop\\dir"));
        DataStreamSource<String> source2 = env.socketTextStream("yonng02", 8888);
        SingleOutputStreamOperator<String> mapStream = source2.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value.startsWith("error"))
                    throw new RuntimeException("date error");
                return value;
            }
        }).setParallelism(4);

        source1.union(mapStream).print();

        env.execute();
    }
}
