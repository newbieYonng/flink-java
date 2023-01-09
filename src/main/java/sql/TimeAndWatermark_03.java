package sql;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class TimeAndWatermark_03 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.enableCheckpointing(8000);


        DataStreamSource<String> source = env.socketTextStream("yonng01", 8888);

        SingleOutputStreamOperator<String> andWatermarks = source.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String in, long recordTimestamp) {
                                return Long.parseLong(in.split(",")[0]);
                            }
                        })
        );

        SingleOutputStreamOperator<Tuple2<Integer, String>> tpStream = andWatermarks.map(new MapFunction<String, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer,String> map(String in) throws Exception {
                String[] fields = in.split(",");
                return Tuple2.of(Integer.parseInt(fields[1]), fields[2]);
            }
        });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = Schema.newBuilder()
                .columnByExpression("uid", $("f0"))
                .columnByExpression("uname", $("f1"))
                .columnByMetadata("e_time", DataTypes.TIMESTAMP(3), "rowtime")
                .build();

        tableEnv.createTemporaryView("v_log", tpStream, schema);

        /*tableEnv.createTemporaryView(
                "v_log",
                tpStream,
                $("uid"), $("uname"), $("e_time").rowtime()
        );*/

        //滚动窗口老API
        /*tableEnv.executeSql(
                "select " +
                        "   uname, " +
                        "   sum(uid) as s_uid, " +
                        "   tumble_end(e_time, interval '5' second) as te " +
                        "from v_log " +
                        "group by uname, tumble(e_time, interval '5' second)"
        ).print();*/

        //滚动窗口新API
        /*tableEnv.executeSql(
                "select " +
                        "   window_start, " +
                        "   window_end, " +
                        "   uname, " +
                        "   sum(uid) as s_uid " +
                        "from table (" +
                        "   tumble(table v_log, descriptor(e_time), interval '5' second)" +
                        ") " +
                        "group by uname, window_start, window_end"
        ).print();*/

        //累计窗口
        tableEnv.executeSql(
                "select " +
                        "   window_start, " +
                        "   window_end, " +
                        "   uname, " +
                        "   sum(uid) as s_uid " +
                        "from table (" +
                        "   cumulate(table v_log, descriptor(e_time), interval '5' second, interval '20' second)" +
                        ") " +
                        "group by uname, window_start, window_end"
        ).print();

        env.execute();
    }
}
