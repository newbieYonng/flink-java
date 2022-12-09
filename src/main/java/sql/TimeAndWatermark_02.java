package sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.math.BigInteger;

import static org.apache.flink.table.api.Expressions.$;

public class TimeAndWatermark_02 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(8000);

        DataStreamSource<String> source = env.socketTextStream("yonng01", 8888);

        SingleOutputStreamOperator<Tuple2<String, BigInteger>> tpStream = source.map(new MapFunction<String, Tuple2<String, BigInteger>>() {
            @Override
            public Tuple2<String, BigInteger> map(String in) throws Exception {
                if (in.startsWith("error")) {
                    throw new RuntimeException("有错误数据,抛出异常");
                }
                String[] fields = in.split(",");
                return Tuple2.of(fields[0], new BigInteger(fields[1]));
            }
        });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = Schema.newBuilder()
                .columnByExpression("tid", $("f0"))
                .columnByExpression("ts", $("f1"))
                .columnByExpression("et", "to_timestamp(from_unixtime(ts/1000))")
                //.columnByExpression("ps", "proctime()")
                //.columnByMetadata("ps", DataTypes.TIMESTAMP_LTZ(), "proctime")
                .build();

        tableEnv.createTemporaryView("t1", tpStream, schema);

        tableEnv.executeSql("select * from t1").print();

        env.execute();
    }
}
