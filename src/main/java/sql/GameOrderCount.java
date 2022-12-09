package sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class GameOrderCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(8000);

        DataStreamSource<String> source = env.socketTextStream("yonng01", 8888);

        SingleOutputStreamOperator<OrderBean> tpStream = source.map(new MapFunction<String, OrderBean>() {
            @Override
            public OrderBean map(String in) throws Exception {
                if (in.startsWith("error")) {
                    throw new RuntimeException("有错误数据,抛出异常");
                }
                String[] fields = in.split(",");
                return new OrderBean(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });

        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env);

        tblEnv.createTemporaryView("v_game_log", tpStream);

        TableResult tableResult = tblEnv.executeSql(
                "select " +
                        "   gameId, " +
                        "   gameId, " +
                        "   sum(money) as tol_monry " +
                        "from v_game_log " +
                        "group by gameId, zoneId "
        );

        tableResult.print();

        env.execute();
    }

    public static class OrderBean {

        public String gameId;
        public String zoneId;
        public Double money;

        public OrderBean(){};

        public OrderBean(String gameId, String zoneId, Double money) {
            this.gameId = gameId;
            this.zoneId = zoneId;
            this.money = money;
        }

        @Override
        public String toString() {
            return "OrderBean{" +
                    "gameId='" + gameId + '\'' +
                    ", zoneId='" + zoneId + '\'' +
                    ", money=" + money +
                    '}';
        }
    }
}
