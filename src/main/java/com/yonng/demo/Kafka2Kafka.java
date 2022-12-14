package com.yonng.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class Kafka2Kafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);
        //将stateBackend保存到HDFS,默认在JobManager中
        //env.setStateBackend(new FsStateBackend("file:///D:\\Develop\\Coding\\IDEA\\flink-java\\data\\backend"));
        env.getCheckpointConfig().setCheckpointStorage("file:///D:\\Develop\\Coding\\IDEA\\flink-java\\data\\backend");
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //任务cancel保留外部存储checkpoint
        //env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties propsConsumer = new Properties();
        propsConsumer.setProperty("bootstrap.servers", "yonng01:9092,yonng02:9092,yonng03:9092");
        propsConsumer.setProperty("group.id", "test02");
        propsConsumer.setProperty("auto.offset.reset", "earliest");
        propsConsumer.setProperty("isolation.level", "read_committed");
        propsConsumer.setProperty("enable.auto.commit", "false");
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                "wc",
                new SimpleStringSchema(),
                propsConsumer);
        //不将偏移量写入kafka的topic中
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        //spark hadoop flink flink
        DataStreamSource<String> source = env.addSource(flinkKafkaConsumer);

        source.print();

        DataStreamSource<String> errorSource = env.socketTextStream("yonng03", 8888);
        DataStream<String> unionStream = errorSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value.startsWith("error"))
                    throw new RuntimeException("--------------------------有误数据---------------------------");
                return value;
            }
        }).union(source);

        SingleOutputStreamOperator<String> filtered = unionStream.filter(e -> !e.startsWith("error"));

        Properties propsProducer = new Properties();
        propsProducer.setProperty("bootstrap.servers", "yonng01:9092,yonng02:9092,yonng03:9092");
        propsProducer.setProperty("group.id", "test03");
        propsProducer.setProperty("transaction.timeout.ms", "60000");

        KafkaSerializationSchema<String> serializationSchema = new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord<>(
                        "my-topic", // target topic
                        element.getBytes(StandardCharsets.UTF_8)); // record contents
            }
        };

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                "my-topic",             // target topic
                serializationSchema,    // serialization schema
                propsProducer,             // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // fault-tolerance

        filtered.addSink(myProducer);

        env.execute();
    }
}
