package com.chi.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

/**
 * @author : chizhou
 * @Description : TODO
 * @date : 2023/5/4 14:53
 * @Version 1.0
 */
public class FlinkMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 默认并行度

        /**
         * 从集合得到数据流
         */
        DataStreamSource<Integer> fromElements = env.fromElements(1, 2, 3, 4, 5);
        SingleOutputStreamOperator<Integer> map = fromElements.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                System.out.println(value);
                return value * 10;
            }
        });/*.print()*/

        CloseableIterator<Integer> integerCloseableIterator = map.executeAndCollect();

        env.execute();
    }
}
