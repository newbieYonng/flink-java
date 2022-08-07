package window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class EventTimeTumblingWindowLateTriggerDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> source = env.socketTextStream("yonng02", 8888);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> mapSteam = source.map(line -> {
            String[] fields = line.split(",");
            long timeStampe = Long.parseLong(fields[0]);
            String word = fields[1];
            int count = Integer.parseInt(fields[2]);
            return Tuple3.of(timeStampe, word, count);
        }).returns(Types.TUPLE(Types.LONG, Types.STRING, Types.INT)).setParallelism(2);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> watermarks = mapSteam.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.seconds(2)) {

            @Override
            public long extractTimestamp(Tuple3<Long, String, Integer> element) {
                return element.f0;
            }
        }).setParallelism(2);

        //设置窗口延迟触发事件
        /*SingleOutputStreamOperator<String> watermarks = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String line) {
                return Long.parseLong(line.split(",")[0]);
            }
        });*/

        /*SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = watermarks.map(line -> {
            //String[] fields = line.split(",");
            return Tuple2.of(line.f1, line.f2);
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).setParallelism(4);*/

        watermarks
                .keyBy(tp -> tp.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(2)
                .print();
 
        env.execute();
    }
}
