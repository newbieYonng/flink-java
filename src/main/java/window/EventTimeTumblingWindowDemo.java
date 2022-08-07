package window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class EventTimeTumblingWindowDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> source = env.socketTextStream("yonng02", 8888);

        //设置窗口延迟触发事件
        SingleOutputStreamOperator<String> watermarks = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String line) {
                return Long.parseLong(line.split(",")[0]);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = watermarks.map(line -> {
            String[] fields = line.split(",");
            return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        wordAndCount
                .keyBy(tp -> tp.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();
 
        env.execute();
    }
}
