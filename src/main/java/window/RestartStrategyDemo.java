package window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RestartStrategyDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
        //env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(30), Time.seconds(2)));
        env.enableCheckpointing(5000);
        DataStreamSource<String> source = env.socketTextStream("yonng02", 8888);

        source.map(new MapFunction<String, String>() {
            @Override
            public String map(String words) throws Exception {
                if (words.startsWith("error")) {
                    throw new RuntimeException("数据有误!!!!!!!!");
                }
                return words;
            }
        }).print();

        env.execute();
    }
}
