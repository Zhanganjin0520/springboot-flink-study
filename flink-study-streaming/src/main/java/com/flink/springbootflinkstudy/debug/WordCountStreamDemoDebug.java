package com.flink.springbootflinkstudy.debug;

import com.flink.springbootflinkstudy.workcount.util.WorkCountData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

import java.time.Duration;


@Slf4j
public class WordCountStreamDemoDebug {

    public static void main(String[] args) throws Exception {
//        final CLI params = CLI.fromArgs(args);
        // 创建流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("127.0.0.1", 8081, "flink-study-streaming/target/flink-study-streaming-0.0.1-SNAPSHOT-WordCountStreamDemoDebug.jar");

        // 创建输入流
        DataStream<String> text = env.fromElements(WorkCountData.WORDS).name("in-memory-input");

        // transform
        DataStream<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)
                .sum(1)
                .name("word-counts");

//        wordCounts.print().name("print-sink");

        // Given an output directory, Flink will write the results to a file
        // using a simple string encoding. In a production environment, this might
        // be something more structured like CSV, Avro, JSON, or Parquet.
        wordCounts.sinkTo(
                        FileSink.<Tuple2<String, Integer>>forRowFormat(
                                        new Path("target/"), new SimpleStringEncoder<>())
                                .withRollingPolicy(
                                        DefaultRollingPolicy.builder()
                                                .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                                .withRolloverInterval(Duration.ofSeconds(10))
                                                .build())
                                .build())
                .name("file-sink");

        // 执行任务
        env.execute("WordCount Stream Demo");
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}

