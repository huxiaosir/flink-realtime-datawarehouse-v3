package org.joisen.app.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.joisen.utils.MyKafkaUtil;
import org.joisen.utils.MysqlUtil;

import java.time.Duration;

/**
 * @Author Joisen
 * @Date 2023/2/7 10:34
 * @Version 1.0
 */
// 数据流：Web/App -> nginx -> 业务服务器（MySQL）-> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> kafka(DWD)
// 程序：   Mock -> Mysql -> Maxwell -> Kafka(ZK) -> DwdTradeOrderPreProcess -> Kafka(ZK) -> DwdTradeOrderDetail -> Kafka(ZK) -> DwdTradePayDetailSuc -> Kafka(ZK)
public class DwdTradePayDetailSuc {
    public static void main(String[] args) throws Exception {

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
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(905));

        // todo 2. 读取 topic_db_0105 数据 并 过滤出支付成功数据
        tableEnv.executeSql(MyKafkaUtil.getTopicDb_0105("pay_detail_suc_0105"));
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "data['payment_type'] payment_type, " +
                "data['callback_time'] callback_time, " +
                "`pt` " +
                "from topic_db_0105 " +
                "where `table` = 'payment_info' "+
                "and `type` = 'update' " +
                "and data['payment_status']='1602'"
        );
        tableEnv.createTemporaryView("payment_info", paymentInfo);
        // 打印测试
//        tableEnv.toAppendStream(paymentInfo, Row.class).print("paymentInfo>>>>");

        // todo 3. 消费下单主题dwd_trade_order_detail_0105数据 存入 dwd_trade_order_detail表  数据来自 DwdTradeOrderDetail
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "sku_num string, " +  // ++
                "order_price string, " +  // ++
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
//                "date_id string, " +
                "create_time string, " +
                "source_id string, " +
                "source_type_id string, " +  // "source_type_code string, " +
                "source_type_name string, " +
//                "sku_num string, " +
//                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string, " +
//                "ts string, " +
                "row_op_ts timestamp_ltz(3) " +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_order_detail_0105", "pay_detail_suc_order_0105"));

        // todo 4. 读取MySQL Basic_Dic表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // todo 5. 三表关联
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "od.id order_detail_id, " +
                "od.order_id, " +
                "od.user_id, " +
                "od.sku_id, " +
                "od.sku_name, " +
                "od.province_id, " +
                "od.activity_id, " +
                "od.activity_rule_id, " +
                "od.coupon_id, " +
                "pi.payment_type payment_type_code, " +
                "dic.dic_name payment_type_name, " +
                "pi.callback_time, " +
                "od.source_id, " +
                "od.source_type_id, " +  //"od.source_type_code, " +
                "od.source_type_name, " +
                "od.sku_num, " +
                "od.order_price, " +
//                "od.split_original_amount, " +
                "od.split_activity_amount, " +
                "od.split_coupon_amount, " +
                "od.split_total_amount split_payment_amount, " +
//                "pi.ts, " +
                "od.row_op_ts row_op_ts " +
                "from payment_info pi " +
                "join dwd_trade_order_detail od " +
                "on pi.order_id = od.order_id " +
                "join `base_dic` for system_time as of pi.pt as dic " +
                "on pi.payment_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // todo 6. 创建Kafka 支付成功表
        tableEnv.executeSql("create table dwd_trade_pay_detail_suc( " +
                "order_detail_id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "payment_type_code string, " +
                "payment_type_name string, " +
                "callback_time string, " +
                "source_id string, " +
                "source_type_id string, " + // "source_type_code string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "order_price string, " +  // ++
//                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_payment_amount string, " +
//                "ts string, " +
                "row_op_ts timestamp_ltz(3), " +
                "primary key(order_detail_id) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_pay_detail_suc_0105"));


        // todo 7. 将数据写出
        tableEnv.executeSql("insert into dwd_trade_pay_detail_suc select * from result_table ");

        // todo 8. 启动任务
//        env.execute("DwdTradePayDetailSuc");



    }
}
