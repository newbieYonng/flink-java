package sql;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class EventTimeWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(30000);

        DataStreamSource<String> source = env.socketTextStream("yonng01", 8888);

        SingleOutputStreamOperator<Row> rowStream = source.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String in) throws Exception {
                String[] fields = in.split(",");
                long time = Long.parseLong(fields[0]);
                String uid = fields[1];
                double money = Double.parseDouble(fields[2]);
                return Row.of(time, uid, money);
            }
        }).returns(Types.ROW(Types.LONG, Types.STRING, Types.DOUBLE)).setParallelism(1);

        SingleOutputStreamOperator<Row> watermarksRow = rowStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Row>() {
                    @Override
                    public long extractTimestamp(Row row, long recordTimestamp) {
                        return (Long) row.getField(0);
                    }
                }));

        tableEnv.createTemporaryView("orders", watermarksRow, $("time"), $("uid"), $("money"), $("etime").rowtime());

        //滚动窗口TUMBLE tumble_start(etime, interval '10' seconds) as win_start
        //Table table = tableEnv.sqlQuery("select uid, sum(money) as tolMoney from orders group by TUMBLE(etime, INTERVAL '10' SECONDS), uid");
        //滑动窗口HOP
        //Table table = tableEnv.sqlQuery("select uid, sum(money) as tolMoney from orders group by hop(etime, interval '2' seconds, INTERVAL '10' SECONDS), uid");
        //会话session窗口CUMULATE
        Table table = tableEnv.sqlQuery("select uid, sum(money) as tolMoney from table(cumulate(table orders, DESCRIPTOR(etime), interval '2' seconds, INTERVAL '10' SECONDS)) group by window_start, window_end, uid");

        DataStream<Row> res = tableEnv.toDataStream(table, Row.class);
        //DataStream<Tuple2<Boolean, Row>> res = tableEnv.toRetractStream(table, Row.class);

        res.print();

        env.execute();
    }
}
