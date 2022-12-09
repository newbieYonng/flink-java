package sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.IntType;

import static org.apache.flink.table.api.Expressions.$;

public class KafkaSourceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> source = env.socketTextStream("yonng01", 8888);
        SingleOutputStreamOperator<Tuple2<Integer, String>> tpStream = source.map(new MapFunction<String, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(String in) throws Exception {
                String[] fields = in.split(",");
                return Tuple2.of(Integer.parseInt(fields[0]), fields[1]);

            }
        });

        /*Schema schema = Schema.newBuilder()
                //.column("id", DataTypes.INT())
                //.column("name", DataTypes.VARCHAR(32))
                .columnByExpression("id", $("f0").cast(DataTypes.INT()))
                .columnByExpression("name", $("f1").cast(DataTypes.VARCHAR(32)))
                .build();*/

        //tableEnv.fromDataStream(tpStream, schema).printSchema();
        tableEnv.createTemporaryView("input",
                tpStream,
                Schema.newBuilder()
                        .columnByExpression("id", $("f0"))
                        .columnByExpression("name", $("f1"))
                        .build());

        Table table = tableEnv.sqlQuery("select sum(id) as cnt_id, name from input group by name");

        /*tableEnv.executeSql(
                "create table dog (" +
                            "`id` integer," +
                            "`name` varchar(32)" +
                        ") with (" +
                            "'connector' = 'jdbc'," +
                            "'driver' = 'com.mysql.jdbc.Driver'," +
                            "'url' = 'jdbc:mysql://localhost:3306/day01'," +
                            "'table-name' = 'dog'," +
                            "'username' = 'root'," +
                            "'password' = 'root'" +
                        ")");

        tableEnv.executeSql("insert into dog select * from input");*/

        tableEnv.executeSql(
                "create table dog (" +
                        "`id` integer," +
                        "`name` varchar(32)" +
                        ") with (" +
                        "'connector' = 'print'" +
                        ")");

        table.executeInsert("dog");

        env.execute();
    }
}
