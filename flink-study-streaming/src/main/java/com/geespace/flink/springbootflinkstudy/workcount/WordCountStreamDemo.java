package com.geespace.flink.springbootflinkstudy.workcount;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;


@Slf4j
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
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        // 按空格拆分单词
                        for (String word : value.split("\\s")) {
                            // 发送单词及其出现次数为1的元组
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }

                })
                // 根据单词进行分组
                .keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                // 计算单词出现的次数
                .sum(1)
                .returns(Types.TUPLE(Types.STRING, Types.INT));
        //Sink输出结果
        wordCounts.addSink(new PrintSinkFunction<Tuple2<String, Integer>>());

        // 执行任务
        env.execute("WordCount Stream Demo");
    }
}

