package sql;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class FunctionsDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("yonng01", 8888);

        SingleOutputStreamOperator<Tuple3<String, Double, Long>> tpStream = source.map(new MapFunction<String, Tuple3<String, Double, Long>>() {
            @Override
            public Tuple3<String, Double, Long> map(String in) throws Exception {
                String[] fields = in.split(",");
                return Tuple3.of(fields[0], Double.parseDouble(fields[1]), Long.parseLong(fields[2]));
            }
        });

        SingleOutputStreamOperator<Tuple3<String, Double, Long>> andWatermarks = tpStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Double, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Double, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Double, Long> in, long recordTimestamp) {
                        return in.f2;
                    }
                }));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table1 = tableEnv.fromDataStream(andWatermarks,
                $("user"), $("url"), $("ts").rowtime());

        tableEnv.createTemporaryView("eventTable", table1);

        /**
         * 标量函数
         */
        /*tableEnv.createTemporarySystemFunction("MyHash", MyHash.class);
        Table table = tableEnv.sqlQuery("select MyHash('222') as h2");*/

        /**
         * 表函数
         */
        /*tableEnv.createTemporarySystemFunction("MySplit", new MySplit());
        Table table = tableEnv.sqlQuery("select " +
                "user, a, b, ts " +
                "from eventTable, lateral table(MySplit(url)) as T(a, b)");*/

        /**
         * 聚合函数
         */
        tableEnv.createTemporarySystemFunction("MyAvg", MyAvgFunction.class);
        Table table = tableEnv.sqlQuery("select " +
                "user, MyAvg(url) as avg_age " +
                "from eventTable " +
                "group by user");


        tableEnv.toChangelogStream(table).print();

        env.execute();
    }

    public static class MyHash extends ScalarFunction {
        public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
            return o.hashCode();
        }
    }

    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class MySplit extends TableFunction<Row> {

        public MySplit() {
        }

        public void eval(String str) {
            String[] fields = str.split("\\?");
            for (int i = 0; i < fields.length; i++) {
                collect(Row.of(fields[i], fields[i].length()));
            }
        }
    }

    public static class MyAvgFunction extends AggregateFunction<Double, Tuple2<Double, Integer>> {

        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        public void accumulate(Tuple2<Double, Integer> acc, Double age) {
            acc.f0 += age;
            acc.f1 += 1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }
    }


}