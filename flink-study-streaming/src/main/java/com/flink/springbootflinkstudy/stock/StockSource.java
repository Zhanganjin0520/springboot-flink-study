//package com.flink.springbootflinkstudy.stock;
//
//import org.apache.flink.api.common.eventtime.Watermark;
//import org.apache.flink.api.connector.source.*;
//import org.apache.flink.core.io.SimpleVersionedSerializer;
//
//public class StockSource implements Source<StockPrice, Watermark> {
//
//    private final String filePath;
//
//    public StockSource(String filePath) {
//        this.filePath = filePath;
//    }
//
//    @Override
//    public SourceReader createReader(SourceReaderContext readerContext) throws Exception {
//        return null;
//    }
//
//    @Override
//    public Boundedness getBoundedness() {
//        return null;
//    }
//
//    @Override
//    public SimpleVersionedSerializer<Watermark> getSplitSerializer() {
//        return null;
//    }
//
//    @Override
//    public SimpleVersionedSerializer getEnumeratorCheckpointSerializer() {
//        return null;
//    }
//
//    @Override
//    public SplitEnumerator restoreEnumerator(SplitEnumeratorContext enumContext, Object checkpoint) throws Exception {
//        return null;
//    }
//
//    @Override
//    public SplitEnumerator createEnumerator(SplitEnumeratorContext enumContext) throws Exception {
//        return null;
//    }
//}
