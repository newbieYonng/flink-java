package sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class AdCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(60000);

        DataStreamSource<String> source = env.socketTextStream("yonng01", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = source.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String in) throws Exception {
                String[] fields = in.split(",");
                return Tuple3.of(fields[0], fields[1], fields[3]);
            }
        });

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        Schema schema = Schema.newBuilder()
                //添加一个字段
                //.columnByExpression("uid", $("f0").cast(DataTypes.TIMESTAMP(3)))
                .build();

        tableEnvironment.createTemporaryView("ad_log", tpStream, schema);

        tableEnvironment.executeSql("desc ad_log").print();

        env.execute();
    }
}
