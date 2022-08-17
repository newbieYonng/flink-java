package state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;

public class KafkaToRedisWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);

        //设置statebackend(设置状态存储的后端),默认JobManager内存中
        env.setStateBackend(new FsStateBackend("hdfs://192.168.77.101:8020/data/checkpoint"));

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("yonng01:9092,yonng02:9092,yonng03:9092")
                .setTopics("wc")
                .setGroupId("wc-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                //取消kafka管理偏移量，不将偏移量写入到kafka特殊的topic中，让flink来管理偏移量
                .setProperty("enable.auto.commit", "false")
                .build();

        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source");

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(tp -> tp.f0).sum(1);

        FlinkJedisClusterConfig config = new FlinkJedisClusterConfig.Builder()
                .setNodes(new HashSet<>(Arrays.asList(new InetSocketAddress("192.168.77.101", 6379))))
                .build();

        res.addSink(new RedisSink<>(config, new RedisWordCountMapper()));

        env.execute();
    }

    public static class RedisWordCountMapper implements RedisMapper<Tuple2<String, Integer>> {

        //指定写入Redis的方式,指定value为hash方式的写入
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "word_count");
        }

        //取出输入数据的key
        @Override
        public String getKeyFromData(Tuple2<String, Integer> stringIntegerTuple2) {
            return stringIntegerTuple2.f0;
        }

        //取出输入数据的value
        @Override
        public String getValueFromData(Tuple2<String, Integer> stringIntegerTuple2) {
            return stringIntegerTuple2.f1.toString();
        }
    }
}
