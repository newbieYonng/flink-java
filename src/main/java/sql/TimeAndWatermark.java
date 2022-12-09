package sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TimeAndWatermark {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tablEnv = StreamTableEnvironment.create(env);

        tablEnv.executeSql(
                "create table table1 (" +
                            "tid int," +
                            "tnme string," +
                            "ts bigint," +
                            "pt as proctime()" +
                            //"et as to_timestamp(from_unixtime(ts/1000))," +
                            //"watermark for et as et - interval '1' second" +
                        ") with (" +
                            "'connector' = 'filesystem'," +
                            "'path' = 'data/ckicks.csv'," +
                            "'format' = 'csv'" +
                        ")");

        Table table = tablEnv.sqlQuery("select * from table1");
        tablEnv.toDataStream(table).print();

        env.execute();
    }
}
