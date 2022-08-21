package com.yonng.demo;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class NonKeyedProcessFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("192.168.77.101", 8888);

        source.process(new ProcessFunction<String, User>() {
            @Override
            public void processElement(String value, Context ctx, Collector<User> out) throws Exception {
                try {
                    User user = JSON.parseObject(value, User.class);
                    out.collect(user);
                } catch (Exception e) {
                    //
                }
            }
        }).print();

        env.execute();
    }

    public static class User {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "User{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}
