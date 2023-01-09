package sql;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class Window_TopN {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> source = env.socketTextStream("yonng01", 8888);

        SingleOutputStreamOperator<String> andWatermarks = source.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String in, long recordTimestamp) {
                                return Long.parseLong(in.split(",")[0]);
                            }
                        }));

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = andWatermarks.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String in) throws Exception {
                String[] fields = in.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = Schema.newBuilder()
                .columnByExpression("tname", $("f0"))
                .columnByExpression("cnt", $("f1"))
                .columnByMetadata("e_time", DataTypes.TIMESTAMP(), "rowtime")
                .build();

        tableEnv.createTemporaryView("v_log", tpStream, schema);


        /*tableEnv.createTemporaryView(
                "v_log",
                tpStream,
                $("tname"), $("cnt"), $("e_time").rowtime());*/

        Table table = tableEnv.sqlQuery("select * " +
                "from (" +
                "   select " +
                "      *, " +
                "      row_number() over(partition by window_start, window_end order by cnt desc) as row_num " +
                "   from (" +
                "       select " +
                "           window_start, window_end, tname, sum(cnt) as cnt" +
                "       from table (" +
                "           tumble(table v_log, descriptor(e_time), interval '10' second)) " +
                "       group by tname, window_start, window_end" +
                "       )" +
                ") where row_num <= 2");

        tableEnv.toChangelogStream(table).print();

        env.execute();
    }
}
