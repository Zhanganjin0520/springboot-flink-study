package com.flink.springbootflinkstudy.stock;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * POJO StockPrice
 * symbol      股票代号
 * ts          时间戳
 * price       价格
 * volume      交易量
 * mediaStatus 媒体对该股票的评价状态
 */
@Getter
@Setter
@ToString
@JsonPropertyOrder({"symbol,ts,volume,price,mediaStatus"})
public class StockPrice {
    public String symbol;
    public String ts;
    public String volume;
    public String price;
    public String mediaStatus;
}
