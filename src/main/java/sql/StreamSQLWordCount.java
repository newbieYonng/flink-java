package sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class StreamSQLWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(20000);

        DataStreamSource<String> source = env.socketTextStream("yonng01", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String in) throws Exception {
                if (in.startsWith("error"))
                    throw new RuntimeException("--------------------ERROR DATA--------------------");
                String[] fields = in.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        //DSL
        Table table = tableEnv.fromDataStream(tpStream, $("id"), $("cnt"));
        Table table1 = table.groupBy($("id")).select($("id"), $("cnt").sum().as("cnt"));
        DataStream<Tuple2<Boolean, Tuple2<String, Integer>>> res = tableEnv.toRetractStream(table1, TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

        DataStream<Tuple2<String, Integer>> tuple2DataStream = tableEnv.toAppendStream(table1, TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
        DataStream<Row> rowDataStream = tableEnv.toDataStream(table1);
        tableEnv.toChangelogStream(table);

        //SQL
        //tableEnv.createTemporaryView("stu", tpStream, $("id"), $("cnt"));
        //TableResult res = tableEnv.executeSql("select id, sum(cnt) as cnt from stu group by id");

        res.print();

        env.execute();
    }
}
