package org.joisen;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.joisen.bean.WaterSensor;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author Joisen
 * @Date 2023/2/4 20:17
 * @Version 1.0
 */
public class Flink06_LookUp_JoinTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 查询MySQL 构建LookUp表  可以无主键 可以数量不一致
        tableEnv.executeSql("create TEMPORARY table base_dic( " +
                "    `dic_code` String, " +
                "    `dic_name` String, " +
                "    `parent_code` String, " +
                "    `create_time` String, " +
                "    `operate_time` String " +
                ") WITH ( " +
                "  'connector' = 'jdbc', " +
                "  'url' = 'jdbc:mysql://hadoop102:3306/gmall_0105', " +
                "  'table-name' = 'base_dic', " +
                "  'driver' = 'com.mysql.cj.jdbc.Driver', " +
                "  'lookup.cache.max-rows' = '10', " + // 缓存数据，在维表数据不变 or 会改变但数据的准确度要求不高的时候使用
                "  'lookup.cache.ttl' = '1 hour', " +
                "  'username' = 'root', " +
                "  'password' = '999719' " +
                ")");

        // 打印LookUp表
//        tableEnv.sqlQuery("select * from base_dic")
//                .execute()
//                .print();

        // 构建事实表
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Double.parseDouble(split[1]),
                            Long.parseLong(split[2]));
                });

        Table table = tableEnv.fromDataStream(waterSensorDS, $("id"),
                $("vc"),
                $("ts"),
                $("pt").proctime());
        tableEnv.createTemporaryView("t1", table);

        // 使用事实表关联维表并打印结果
        tableEnv.sqlQuery("select " +
                "    t1.id, " +
                "    t1.vc, " +
                "    dic.dic_name " +
                "from t1 " +
                "join base_dic FOR SYSTEM_TIME AS OF t1.pt as dic " +
                "on t1.id=dic.dic_code").execute().print();


    }
}
