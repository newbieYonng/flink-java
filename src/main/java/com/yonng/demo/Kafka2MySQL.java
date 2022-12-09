package com.yonng.demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Kafka2MySQL {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.enableCheckpointing(6000);
        env.getCheckpointConfig().setCheckpointStorage("file:\\D:\\Develop\\Coding\\IDEA\\flink-java\\data\\backend");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("yonng01:9092,yonng02:9092,yonng03:9092")
                .setTopics("wc")
                .setGroupId("wc01")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                //checkpoint时不将偏移量提交到kafka特殊的topic中
                .setProperty("commit.offsets.on.checkpoint", "false")
                .build();

        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<String> words = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String in, Collector<String> out) throws Exception {
                String[] fields = in.split(" ");
                for (String field : fields) {
                    out.collect(field);
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String in) throws Exception {
                return Tuple2.of(in, 1);
            }
        }).keyBy(tp -> tp.f0).sum(1);

        SinkFunction<Tuple2<String, Integer>> MysqlSink = JdbcSink.sink(
                "insert into wc values (?, ?) ON DUPLICATE KEY UPDATE count = ?",
                new JdbcStatementBuilder<Tuple2<String, Integer>>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Tuple2<String, Integer> tp) throws SQLException {
                        preparedStatement.setString(1, tp.f0);
                        preparedStatement.setInt(2, tp.f1);
                        preparedStatement.setInt(3, tp.f1);
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/testdb?characterEncoding=utf-8")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root")
                        .build()
        );

        res.addSink(MysqlSink);

        env.execute();
    }
}
