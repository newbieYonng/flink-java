package com.yonng.demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSourceDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        //设置Evn的并行度
        env.setParallelism(2);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("yonng01:9092,yonng02:9092,yonng03:9092")
                .setTopics("tpc01")
                .setGroupId("test01")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source");
        source.print();

        /*Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "yonng01:9092,yonng02:9092,yonng02:9092");
        properties.setProperty("group.id", "test777");
        properties.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                "tpc01",
                new SimpleStringSchema(),
                properties
        );

        DataStreamSource<String> source = env.addSource(flinkKafkaConsumer);
        source.print();*/

        env.execute();
    }
}
