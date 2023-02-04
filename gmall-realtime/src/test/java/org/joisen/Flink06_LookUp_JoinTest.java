package org.joisen;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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
                "  'username' = 'root', " +
                "  'password' = '999719' " +
                ")");

        // 打印LookUp表
        tableEnv.sqlQuery("select * from base_dic")
                .execute()
                .print();


    }
}
