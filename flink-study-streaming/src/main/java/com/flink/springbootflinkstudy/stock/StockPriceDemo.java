//package com.flink.springbootflinkstudy.stock;
//
//import org.apache.flink.connector.file.src.FileSource;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//
///**
// * This demo shows how to get the max price of a stock data stream.
// */
//@SuppressWarnings("serial")
//public class StockPriceDemo {
//
//    public static void main(String[] args) throws Exception {
//
//        // get Execution Environment
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//// Optionally set the auto watermark interval (if needed)
//        env.getConfig().setAutoWatermarkInterval(200);
//
////        DataStream<StockPrice> stream = env.addSource(new StockSource("stock/stock-tick-20200108.csv"));
//
//        // Create a source using the new Source API
//        SourceFunction<StockPrice> stockSource = new StockSource("stock/stock-tick-20200108.csv");
//
//// Define a watermark strategy if needed
//        WatermarkStrategy<StockPrice> watermarkStrategy = WatermarkStrategy
//                .<StockPrice>forMonotonousTimestamps()
//                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());
//
//// Create the data stream using fromSource method
////        DataStream<StockPrice> stream = env
////                .fromSource(stockSource, watermarkStrategy, "Stock Price Source");
//
////        DataStream<StockPrice> stream1 = env
////                .read(format, "stock/stock-tick-20200108.csv")
////                .map(new StockPriceParser());
//
//
////        DataStream<StockPrice> maxStream = stream
////                .keyBy("symbol")
////                .max("price");
//
//        maxStream.print();
//
//        env.execute("stock price");
//    }
//}
