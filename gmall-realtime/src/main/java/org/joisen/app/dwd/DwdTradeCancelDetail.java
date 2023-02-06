package org.joisen.app.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.joisen.utils.MyKafkaUtil;

/**
 * @Author Joisen
 * @Date 2023/2/6 17:04
 * @Version 1.0
 */
// 数据流： Web/App  -> nginx  -> 业务服务器（MySQL） ->  Maxwell  -> Kafka(ODS)  ->  FlinkApp  ->  Kafka(DWD)  -> FlinkApp  -> Kafka(DWD)
// 程序：      Mock -> Mysql  -> Maxwell  ->  Kafka(ZK)  ->  DwdTradeOrderPreProcess  -> Kafka(ZK)  ->  DwdTradeCancelDetail  ->  Kafka(ZK)
public class DwdTradeCancelDetail {
    public static void main(String[] args) {

        // todo 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);// 生产环境中设置为kafka主题的分区数
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // todo 1.1 开启CheckPoint
//        env.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));

        // todo 1.2 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/0105/ck");
//        System.setProperty("HADOOP_USER_NAME", "joisen");

        // todo 1.3 设置状态的TTL  生产环境中设置为最大的乱序程度    进行join的数据是存在状态中等待要join的数据的到来
//        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // todo 2. 读取kafka订单预处理主题 dwd_trade_order_pre_process_0105 数据 创建表
        tableEnv.executeSql("" +
                "create table dwd_order_pre( " +
                "    `id` string, " +
                "    `order_id` string, " +
                "    `sku_id` string, " +
                "    `sku_name` string, " +
                "    `order_price` string, " +
                "    `sku_num` string, " +
                "    `create_time` string, " +
                "    `source_type_id` string, " +
                "    `source_type_name` string, " +
                "    `source_id` string, " +
                "    `split_total_amount` string, " +
                "    `split_activity_amount` string, " +
                "    `split_coupon_amount` string, " +
                "    `consignee` string, " +
                "    `consignee_tel` string, " +
                "    `total_amount` string, " +
                "    `order_status` string, " +
                "    `user_id` string, " +
                "    `payment_way` string, " +
                "    `delivery_address` string, " +
                "    `order_comment` string, " +
                "    `out_trade_no` string, " +
                "    `trade_body` string, " +
                "    `operate_time` string, " +
                "    `expire_time` string, " +
                "    `process_status` string, " +
                "    `tracking_no` string, " +
                "    `parent_order_id` string, " +
                "    `province_id` string, " +
                "    `activity_reduce_amount` string, " +
                "    `coupon_reduce_amount` string, " +
                "    `original_total_amount` string, " +
                "    `feight_fee` string, " +
                "    `feight_fee_reduce` string, " +
                "    `refundable_time` string, " +
                "    `order_detail_activity_id` string, " +
                "    `activity_id` string, " +
                "    `activity_rule_id` string, " +
                "    `order_detail_coupon_id` string, " +
                "    `coupon_id` string, " +
                "    `coupon_use_id` string, " +
                "    `type` string, " +
                "    `old` map<string,string> " +
                ") " + MyKafkaUtil.getKafkaDDL("dwd_trade_order_pre_process_0105", "cancel_detail_0105"));

        // todo 3. 过滤出取消订单数据
        Table filteredTable = tableEnv.sqlQuery("" +
                "select " +
                "id, " +
                "order_id, " +
                "user_id, " +
                "sku_id, " +
                "sku_name, " +
                "province_id, " +
                "activity_id, " +
                "activity_rule_id, " +
                "coupon_id, " +
//                "operate_date_id date_id, " +
                "operate_time cancel_time, " +
                "source_id, " +
                "source_type_id, " + //"source_type source_type_code, " +
                "source_type_name, " +
                "sku_num, " +
                "order_price, " +  // ++
//                "split_original_amount, " +
                "split_activity_amount, " +
                "split_coupon_amount, " +
                "split_total_amount " +  // 删除 ，
//                "oi_ts ts, " +
//                "row_op_ts " +
                "from dwd_order_pre " +
                "where `type` = 'update' " +
                "and `old`['order_status'] is not null " +
                "and order_status = '1003'");
        tableEnv.createTemporaryView("filtered_table", filteredTable);


        // todo 4. 创建kafka 取消订单表
        tableEnv.executeSql("create table dwd_trade_cancel_detail( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
//                "date_id string, " +
                "cancel_time string, " +
                "source_id string, " +
                "source_type_id string, " +  //"source_type_code string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "order_price string, " +  // ++
//                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string " + // 删除 ，
//                "ts string, " +
//                "row_op_ts timestamp_ltz(3) " +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_cancel_detail_0105"));


        // todo 5. 将数据写出到Kafka
        tableEnv.executeSql(" insert into dwd_trade_cancel_detail select * from filtered_table ");


    }
}
