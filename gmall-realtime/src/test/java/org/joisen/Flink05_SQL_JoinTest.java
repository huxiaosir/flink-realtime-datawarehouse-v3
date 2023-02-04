package org.joisen;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.joisen.bean.WaterSensor;
import org.joisen.bean.WaterSensor2;

import java.time.Duration;

/**
 * @Author Joisen
 * @Date 2023/2/4 17:06
 * @Version 1.0
 */
public class Flink05_SQL_JoinTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        System.out.println(tableEnv.getConfig().getIdleStateRetention());

        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        System.out.println(tableEnv.getConfig().getIdleStateRetention());

        SingleOutputStreamOperator<WaterSensor> waterSensorDS1 = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Double.parseDouble(split[1]),
                            Long.parseLong(split[2]));
                });
        SingleOutputStreamOperator<WaterSensor2> waterSensorDS2 = env.socketTextStream("hadoop102", 8889)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor2(split[0],
                            split[1],
                            Long.parseLong(split[2]));
                });

        // 将流转换为动态表
        tableEnv.createTemporaryView("t1", waterSensorDS1);
        tableEnv.createTemporaryView("t2", waterSensorDS2);

        // FlinkSQLJOIN

        // inner join 更新状态时间 左表: OnCreateAndWrite   右表：OnCreateAndWrite
//        tableEnv.sqlQuery("select t1.id,t1.vc,t2.id,t2.name from t1 join t2 on t1.id=t2.id")
//                .execute()
//                .print();

        // left join  更新状态时间 左表:OnReadAndWrite   右表：OnCreateAndWrite
//        tableEnv.sqlQuery("select t1.id,t1.vc,t2.id,t2.name from t1 left join t2 on t1.id=t2.id")
//                .execute()
//                .print();

        // right join  更新状态时间 左表:OnCreateAndWrite   右表：OnReadAndWrite
        tableEnv.sqlQuery("select t1.id,t1.vc,t2.id,t2.name from t1 right join t2 on t1.id=t2.id")
                .execute()
                .print();
    }
}
