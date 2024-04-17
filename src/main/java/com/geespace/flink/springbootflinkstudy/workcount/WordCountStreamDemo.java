package com.geespace.flink.springbootflinkstudy.workcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamDemo {

    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 输入数据源，这里使用一个固定的字符串数组作为输入流
        String[] lines = {"hello world", "hello flink", "world world"};

        // 创建输入流
        DataStream<String> textStream = env.fromElements(lines);

        // 单词拆分并计数
        DataStream<Tuple2<String, Integer>> wordCounts = textStream
                // 将每行文本按空格拆分成单词
                .flatMap(new Tokenizer())
                // 根据单词进行分组
                .keyBy(0)
                // 计算单词出现的次数
                .sum(1);

        // 输出结果
        wordCounts.print();

        // 执行任务
        env.execute("WordCount Stream Demo");
    }

    // 自定义拆分函数，用于将文本按空格拆分成单词
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 按空格拆分单词
            for (String word : value.split("\\s")) {
                // 发送单词及其出现次数为1的元组
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}

