package org.joisen;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.joisen.bean.WaterSensor;
import org.joisen.bean.WaterSensor2;
import scala.Tuple2;

/**
 * @Author Joisen
 * @Date 2023/2/4 16:33
 * @Version 1.0
 */
public class Flink04_DataStreamJoinTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS1 = env.socketTextStream("hadoop102", 8888)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String s, long l) {
                String[] split = s.split(",");
                return new Long(split[2]) * 1000L;
            }
        })).map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0],
                    Double.parseDouble(split[1]),
                    Long.parseLong(split[2]));
        });

        SingleOutputStreamOperator<WaterSensor2> waterSensorDS2 = env.socketTextStream("hadoop102", 8889)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] split = element.split(",");
                        return new Long(split[2]) * 1000L;
                    }
                })).map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor2(split[0],
                            split[1],
                            Long.parseLong(split[2]));
                });

        // join
        SingleOutputStreamOperator<Tuple2<WaterSensor, WaterSensor2>> result = waterSensorDS1.keyBy(WaterSensor::getId)
                .intervalJoin(waterSensorDS2.keyBy(WaterSensor2::getId))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<WaterSensor, WaterSensor2, Tuple2<WaterSensor, WaterSensor2>>() {
                    @Override
                    public void processElement(WaterSensor left, WaterSensor2 right, ProcessJoinFunction<WaterSensor, WaterSensor2, Tuple2<WaterSensor, WaterSensor2>>.Context ctx, Collector<Tuple2<WaterSensor, WaterSensor2>> out) throws Exception {
                        out.collect(new Tuple2<>(left, right));
                    }
                });

        result.print(">>>>");
        env.execute();

    }
}
