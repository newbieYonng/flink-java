package window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class EventTimeSlidingWindowDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> source = env.socketTextStream("yonng02", 8888);

        SingleOutputStreamOperator<String> watermarks = source.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        return Long.parseLong(element.split(",")[0]);
                    }
                }));

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = watermarks.map(line -> {
            String[] fields = line.split(",");
            return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        mapStream
                .keyBy(tp -> tp.f0)
                //.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(1)
                .print();

        env.execute();
    }
}
