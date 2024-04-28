package com.chi.demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author : chizhou
 * @Description : TODO
 * @date : 2023/5/11 09:08
 * @Version 1.0
 */
public class FlinkWatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(5000);
        env.setParallelism(1);

        // 1,1000
        SingleOutputStreamOperator<String> inputStream =
                env.socketTextStream("localhost", 9999).disableChaining();

        SingleOutputStreamOperator<String> watermarksStream =
                inputStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<String>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner(
                                        (element, recordTimestamp) -> Long.parseLong(element.split(",")[1]))
                );

        SingleOutputStreamOperator<String> resultStream = watermarksStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value,
                                       ProcessFunction<String, String>.Context ctx,
                                       Collector<String> out) throws Exception {

                long watermark = ctx.timerService().currentWatermark();
                long processTime = ctx.timerService().currentProcessingTime();

                String record = "当前数据: (" + value + ")" +
                        "当前时间: (" + processTime + ")" +
                        "当前水印: (" + watermark + ")";

                out.collect(record);
            }
        }).disableChaining();

        resultStream.print();

        env.execute();
    }
}
