package com.flink.springbootflinkstudy.stock;

import java.io.File;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * This demo shows how to get the max price of a stock data stream.
 */
public class StockPriceDemo {

    public static void main(String[] args) throws Exception {

        // get Execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Watermark 窗口
        env.getConfig().setAutoWatermarkInterval(200);

        //读取文件
        CsvSchema csvSchema = new CsvMapper().schemaFor(StockPrice.class).withoutQuoteChar().withColumnSeparator(',');
        CsvReaderFormat<StockPrice> csvFormat = CsvReaderFormat.forSchema(csvSchema, TypeInformation.of(StockPrice.class));
        BulkFormat<StockPrice, FileSourceSplit> bulkFormat = new StreamFormatAdapter<>(csvFormat);
        FileSource<StockPrice> fileSource = FileSource.forBulkFileFormat(bulkFormat, Path.fromLocalFile(new File("stock/stock-tick-20200108.csv"))).build();

        //stock source
        DataStreamSource<StockPrice> stockSource
                = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "stock source");
        stockSource.print();

        env.execute("stock price");
    }
}
