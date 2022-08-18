package com.yonng.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JDBCSinkDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> source = env.socketTextStream("192.168.77.103", 8888);

        SingleOutputStreamOperator<Tuple2<Integer, String>> tpStream = source.map(new MapFunction<String, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(Integer.parseInt(fields[0]), fields[1]);
            }
        });

        tpStream.addSink(
                JdbcSink.sink(
                        "insert into dog (id, name) values (?, ?)",
                        new JdbcStatementBuilder<Tuple2<Integer, String>>() {
                            @Override
                            public void accept(PreparedStatement preparedStatement, Tuple2<Integer, String> tp) throws SQLException {
                                //绑定参数
                                preparedStatement.setInt(1, tp.f0);
                                preparedStatement.setString(2, tp.f1);
                            }
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(2000)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://localhost:3306/day01?serverTimezone=UTC")
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("root")
                                .build()
                )
        );

        env.execute();
    }
}
